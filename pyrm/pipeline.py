from pyrm.formulation import (
    get_ca_formulation_data,
    get_sg_formulation_data,
    upload_formulation_data,
)


def run_formulation_pipeline(days=3):

    print("****** Pipeline Starting ******")
    df1 = get_ca_formulation_data(days)
    res = upload_formulation_data(df1, mfg_site="CA")

    df2 = get_sg_formulation_data(days)
    res = upload_formulation_data(df2, mfg_site="SG")
    print("****** Pipeline Completed ******")
