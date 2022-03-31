import pandas as pd
from datetime import date, timedelta
from pybox import box_create_df_from_files, get_box_client
from pydb import get_postgres_connection, batch_upload_df


def parse_formulation_br(file):
    try:
        tmp = pd.read_excel(
            file,
            sheet_name="Data",
            engine="openpyxl",
            dtype={
                "PN": str,
                "LN": str,
                "tag": str,
                "data type": str,
                "tag": str,
                "value": str,
                "UOM": str,
                "part number": str,
                "lot number": str,
            },
        )
        tmp["PN"] = tmp["PN"].str.strip()
        tmp["LN"] = tmp["LN"].str.strip()
    except:
        print(f"### --- {file} skipped --- ###")
        tmp = pd.DataFrame()

    return tmp


def get_sg_formulation_data(days=3):

    last_modified_date = str(date.today() - timedelta(days=days))
    print(f"Looking for new data since {last_modified_date} ....")

    client = get_box_client()

    df = box_create_df_from_files(
        box_client=client,
        last_modified_date=last_modified_date,
        box_folder_id="126509184456",
        file_extension="xlsx",
        file_pattern="",
        file_parsing_functions=parse_formulation_br,
    )

    df = df.reset_index()

    return df


def get_ca_formulation_data(days=3):

    last_modified_date = str(date.today() - timedelta(days=days))
    print(f"Looking for new data since {last_modified_date} ....")

    client = get_box_client()

    df = box_create_df_from_files(
        box_client=client,
        last_modified_date=last_modified_date,
        box_folder_id="110045093581",
        file_extension="xlsx",
        file_pattern="",
        file_parsing_functions=parse_formulation_br,
    )

    df = df.reset_index()

    return df


def upload_formulation_data(dfs, mfg_site):

    if dfs.shape[0] < 1:
        print("No data to upload")
        return 1

    conn = get_postgres_connection(
        service_name="cpdda-postgres", username="cpdda", db_name="cpdda"
    )

    print("Creating the product master dataframe")
    product_master = dfs[dfs["tag"] == "product name"]
    product_master = product_master.drop_duplicates(subset=["PN"])
    product_master = product_master[["PN", "value"]]
    product_master = product_master.rename(columns={"value": "pn_desc", "PN": "pn"})

    # Business logic to check already existing PNs
    df = pd.read_sql("SELECT pn from form.product_master;", con=conn)
    pns_in_db = df.pn.tolist()

    print(f"Removing product already in the database")
    product_master = product_master[~product_master.pn.isin(pns_in_db)]
    print(f"No of new products found {len(product_master)}")

    # Upload product data to DB
    print("Uploading product master to DB")
    res = batch_upload_df(
        conn=conn,
        df=product_master,
        tablename="form.product_master",
        insert_type="insert_only",
    )

    ## CREATE LOT MASTER DATAFRAME ------------------------------------
    print("Creating lost master dataframe")
    lot_master = dfs[dfs["data type"] == "batch summary"]
    lot_master = lot_master[["PN", "LN", "tag", "value"]]
    lot_master = lot_master.rename(
        columns={"PN": "pn", "LN": "ln", "tag": "data_name", "value": "data_value"}
    )
    lot_master = lot_master[
        lot_master["data_name"].isin(
            ["manufacture date", "expiration date", "manufactured by", "IPT date"]
        )
    ]
    lot_master = lot_master.drop_duplicates(subset=["ln", "data_name"], keep="last")
    lot_master = lot_master.pivot(
        values="data_value", index=["pn", "ln"], columns="data_name"
    )
    lot_master.reset_index(inplace=True)
    lot_master = lot_master.rename(
        columns={
            "IPT date": "ipt_date",
            "expiration date": "exp_date",
            "manufacture date": "mfg_date",
            "manufactured by": "mfg_by",
        }
    )
    lot_master["mfg_by"] = lot_master["mfg_by"].str[:19]
    lot_master["ipt_date"] = lot_master["ipt_date"].str[:19]
    lot_master["ln"] = lot_master["ln"].astype(str)
    lot_master["ln"] = lot_master["ln"].str[:11]
    lot_master = lot_master.assign(mfg_site=mfg_site)

    new_lots = tuple(lot_master.ln.unique().astype(str))
    cur = conn.cursor()

    if len(new_lots) <= 1:
        cur.execute(f"DELETE FROM form.lot_master WHERE lot_no = '{new_lots[0]}';")
        conn.commit()
    else:
        cur.execute(f"DELETE FROM form.lot_master WHERE lot_no IN {new_lots};")
        conn.commit()

    # cur.execute(f"DELETE FROM form.lot_master WHERE lot_no IN {new_lots};")
    # conn.commit()

    print(f"No of new lots found {len(lot_master)}")

    if len(lot_master) > 0:
        df = pd.read_sql("SELECT pn, id from form.product_master;", con=conn)
        lot_master["pn"] = lot_master["pn"].astype(str)
        lot_master = pd.merge(lot_master, df, on="pn")
        lot_master = lot_master.drop(columns=["pn"])
        lot_master = lot_master.rename(columns={"id": "pn_id", "ln": "lot_no"})
        lot_master = lot_master.fillna("")
        # Upload lot data to DB
        print("Uploading lot master to DB")
        res = batch_upload_df(
            conn=conn,
            df=lot_master,
            tablename="form.lot_master",
            insert_type="insert_only",
        )

    ## CREATE LINEAGE DATAFRAME ---------------------------------
    print("Creating lineage dataframe")
    lineage = dfs[dfs["data type"] == "formulation"]
    lineage = lineage[["PN", "LN", "tag", "part number", "lot number"]]
    lineage = lineage.rename(
        columns={
            "PN": "to_pn",
            "LN": "to_ln",
            "tag": "from_desc",
            "part number": "from_pn",
            "lot number": "from_ln",
        }
    )
    lineage = lineage.dropna()
    # lineage = lineage[~lineage["to_ln"].isin(lns_in_db)]

    lot_id = pd.read_sql("SELECT lot_no, id from form.lot_master;", con=conn)
    pn_id = pd.read_sql("SELECT pn, id from form.product_master;", con=conn)
    # add lot ids
    lineage["to_ln"] = lineage["to_ln"].astype(str)
    lineage = pd.merge(lineage, lot_id, left_on="to_ln", right_on="lot_no")
    lineage = lineage.drop(columns=["to_ln", "lot_no"])
    lineage = lineage.rename(columns={"id": "to_ln_id"})
    # add pn ids
    lineage["to_pn"] = lineage["to_pn"].astype(str)
    lineage = pd.merge(lineage, pn_id, left_on="to_pn", right_on="pn")
    lineage = lineage.drop(columns=["to_pn", "pn"])
    lineage = lineage.rename(columns={"id": "to_pn_id"})
    lineage = lineage.dropna()
    print(f"No of new lineage relationships found {len(lineage)}")

    # Upload lineage data to DB
    print("Uploading lineage to DB")
    res = batch_upload_df(
        conn=conn, df=lineage, tablename="form.lineage", insert_type="insert_only"
    )

    ## CREATE IPT DATAFRAME ---------------------------------------
    print("Creating IPT dataframe")
    ipt = dfs[
        dfs["data type"].isin(
            [
                "IPT value",
                "IPT Value",
                "IPT Results",
                "spec and IPT results",
                "Spec and IPT result",
                "In-Process test",
            ]
        )
    ]
    ipt = ipt[["PN", "LN", "tag", "value", "UOM"]]
    ipt = ipt.rename(
        columns={
            "PN": "pn",
            "LN": "ln",
            "tag": "data_name",
            "value": "data_value",
            "UOM": "uom",
        }
    )
    ipt = ipt[ipt["data_value"].notnull()]
    # ipt = ipt[~ipt.ln.isin(lns_in_db)]
    ipt["data_name"] = ipt["data_name"].str.strip()
    df_unclean = ipt[ipt["data_name"].str.match(r"(.*) \d$")]
    df_unclean["data_name"] = df_unclean["data_name"].str.extract(r"(.*) \d$")
    df_clean = ipt[~ipt["data_name"].str.match(r"(.*) \d$")]
    ipt = df_clean.append(df_unclean)

    lot_id = pd.read_sql("SELECT lot_no, id from form.lot_master;", con=conn)
    pn_id = pd.read_sql("SELECT pn, id from form.product_master;", con=conn)
    # add lot ids
    ipt["ln"] = ipt["ln"].astype(str)
    ipt = pd.merge(ipt, lot_id, left_on="ln", right_on="lot_no")
    ipt = ipt.drop(columns=["ln", "lot_no"])
    ipt = ipt.rename(columns={"id": "ln_id"})
    # add pn ids
    ipt["pn"] = ipt["pn"].astype(str)
    ipt = pd.merge(ipt, pn_id, left_on="pn", right_on="pn")
    ipt = ipt.drop(columns=["pn"])
    ipt = ipt.rename(columns={"id": "pn_id"})
    ipt = ipt.dropna()
    ipt["uom"] = ipt["uom"].str.slice(0, 24)
    print(f"No of new ipt relationships found {len(ipt)}")

    # Upload ipt data to DB
    print("Uploading IPT to DB")
    res = batch_upload_df(
        conn=conn, df=ipt, tablename="form.ipt", insert_type="insert_only"
    )
    print("------- Upload Complete ------")
