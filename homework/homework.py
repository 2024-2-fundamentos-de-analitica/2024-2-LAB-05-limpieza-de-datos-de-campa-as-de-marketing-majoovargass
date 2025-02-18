"""
Escriba el codigo que ejecute la accion solicitada.
"""
import pandas as pd
import zipfile
from pathlib import Path
import os
import glob
# pylint: disable=import-outside-toplevel

def load_input(input):

    dtf = []

    inpf = Path(input)

    for archivillo in inpf.iterdir():

        with zipfile.ZipFile(archivillo) as zf:
                for csv_file in zf.namelist():
                    with zf.open(csv_file) as file:
                        dfp = pd.read_csv(file)
                        dtf.append(dfp)
    
    dtframe = pd.concat(dtf, ignore_index=True)

    return dtframe

def save_output(dataframe1, dataframe2, dataframe3, outdic, output_name1, output_name2, output_name3):
    """Save output to a file."""

    if os.path.exists(outdic):
        files = glob.glob(f"{outdic}/*")
        for file in files:
            os.remove(file)
        os.rmdir(outdic)

    os.makedirs(outdic)

    dataframe1.to_csv(
        f"{outdic}/{output_name1}",
        sep=",",
        index=False,
        header=True,
    )
    dataframe2.to_csv(
        f"{outdic}/{output_name2}",
        sep=",",
        index=False,
        header=True,
    )

    dataframe3.to_csv(
        f"{outdic}/{output_name3}",
        sep=",",
        index=False,
        header=True,
    )



def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    dfp = load_input('files/input')
    #cleaning data from dfp

    #cambios en la columna 'job'
    dfp['job'] = dfp['job'].str.replace('.','', regex=False).str.replace('-','_', regex=False)

    #cambios en la columna education
    dfp['education'] = dfp['education'].str.replace('.','_', regex=False)
    dfp['education'] = dfp['education'].replace('unknown',pd.NA)

    #cambios en la columna credit_default
    dfp['credit_default'] = dfp['credit_default'].replace('yes',1, regex=False)
    dfp.loc[dfp['credit_default'] != 1, 'credit_default'] = 0

    #cambios en la columna mortgage
    dfp['mortgage'] = dfp['mortgage'].replace('yes',1, regex=False)
    dfp.loc[dfp['mortgage'] != 1, 'mortgage'] = 0

    #cambios en previous_outcome
    dfp['previous_outcome'] = dfp['previous_outcome'].replace('success',1)
    dfp.loc[dfp['previous_outcome'] != 1, 'previous_outcome'] = 0

    #cambios en campaign_outcome
    dfp['campaign_outcome'] = dfp['campaign_outcome'].replace('yes',1)
    dfp.loc[dfp['campaign_outcome'] != 1, 'campaign_outcome'] = 0

    #creacion de last_contact_day
    meses_a_numeros = {
    'mar': '03',
    'apr': '04',
    'may': '05',
    'jun': '06',
    'jul': '07',
    'aug': '08',
    'sep': '09',
    'oct': '10',
    'nov': '11',
    'dec': '12'
    }

    dfp['month'] = dfp['month'].map(meses_a_numeros)
    dfp['last_contact_date'] = '2022' + '-' + dfp['month'] + '-' + dfp['day'].astype(str)

    #crear los 3 dfp
    client_dfp = dfp.iloc[:, 1:8 ]

    campaign_dfp = dfp.iloc[:, [1,10,11,12,13,16,17]]

    economics_dfp = dfp.iloc[:, [1,14,15]]

    save_output(client_dfp, campaign_dfp, economics_dfp, 'files/output', 'client.csv', 'campaign.csv', 'economics.csv')


if __name__ == "__main__":
    clean_campaign_data()