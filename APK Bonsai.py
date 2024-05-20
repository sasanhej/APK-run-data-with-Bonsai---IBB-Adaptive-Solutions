import oracledb
import pandas as pd
import pyexasol
import numpy as np


query = '''
SELECT
	****.A_SIMULATIONUMSATZ.SCENARIO_ID AS "Scenario ID",
	****.A_LIEFERANT.EXTERNALID AS "Supplier Name",
	****.A_UMSATZBASIS.NAME AS "Turnonver Basis",
	****.A_SIMULATIONUMSATZ.KONDDATUM AS "DATE",
	SUM(****.A_SIMULATIONUMSATZ.PLANUMSATZLIEFERANT) AS "Turnover Planned Amount (€)"
FROM 
	****.A_SIMULATIONUMSATZ
	INNER JOIN ****.A_SIMULATIONSCENARIO
		ON ****.A_SIMULATIONSCENARIO.ID = ****.A_SIMULATIONUMSATZ.SCENARIO_ID 
	INNER JOIN ****.A_LIEFERANT
		ON ****.A_LIEFERANT.ID = ****.A_SIMULATIONUMSATZ.LIEFERANT_ID
	INNER JOIN ****.A_UMSATZBASIS
		ON ****.A_UMSATZBASIS.ID = ****.A_SIMULATIONUMSATZ.UMSATZBASIS_ID
WHERE
	****.A_SIMULATIONUMSATZ.DTYPE = 0
	AND ****.A_SIMULATIONUMSATZ.PLANUMSATZLIEFERANT IS NOT NULL
	AND ****.A_SIMULATIONSCENARIO.GUELTIGESSZENARIO = '1'
GROUP BY
	****.A_SIMULATIONUMSATZ.SCENARIO_ID,
	****.A_LIEFERANT.EXTERNALID,
	****.A_UMSATZBASIS.NAME,
	****.A_SIMULATIONUMSATZ.KONDDATUM
'''


def main():
    connection = oracledb.connect(user="****", password=f"****", host="****", port=0000, service_name="****")

    cursor = connection.cursor()

    df = pd.DataFrame(cursor.execute(query))

    headers = ["Scenario ID","Supplier Name","Turnonver Basis","Date","Turnover Planned Amount (€)"]
    
    df.columns = headers

    measurer = np.vectorize(len)

    lens = measurer(df.values.astype(str)).max(axis=0)

    tblstr = ''
    for index in range(df.shape[1]):
        colstr = f'\n"{df.columns[index]}" VARCHAR({lens[index]}),'
        tblstr += colstr

    querstr = f'''CREATE OR REPLACE TABLE WORKSPACE_SCM.BONSAI_PLAN (\n{tblstr[:-1]})'''

    C = pyexasol.connect(dsn='****:0000', user='****', password='****!', compression=True)
    C.execute(querstr)
    C.import_from_pandas(src=df,table=('****', '****'))


if __name__ == "__main__":
    main()