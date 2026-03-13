#!/usr/bin/env python
# coding: utf-8
import sys
import findspark
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import argparse
import calendar
import os
import time  # <-- ADICIONADO: Para medir o tempo (wall-clock)
from datetime import date, timedelta, datetime, time as dt_time
from dateutil.relativedelta import relativedelta

findspark.init()

if __name__ == '__main__':
    # --- INÍCIO DA MEDIÇÃO DE TEMPO ---
    t_start = time.time()  # [cite: 73]

    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--month", nargs='?', help="month")
    parser.add_argument("-y", "--year", nargs='?', help="year")
    parser.add_argument("-s", "--start", nargs='?', help="start day")
    parser.add_argument("-o", "--outfile", nargs='?', help="outfile")
    args = parser.parse_args()

    DATADIR = '/projects/F202500010HPCVLABUMINHO/DataSets/Reports/2025'
    OUTDIR = '/projects/F202500010HPCVLABUMINHO/uminhocp043/DATA'  # <-- ATUALIZADO com a tua pasta

    # (Para não encher o ecrã, mantém o teu dicionário 'params' gigante exatamente como estava no original)
    params = {
        'reportPeriod': 0, 'reportPeriodTrimester': 0, 'reportPeriodYear': 0,
        'reportMonth': 0, 'reportYear': 0, 'armnodes': 1632, 'amdnodes': 500, 'gpunodes': 132,
        'percentaviail': 0.8, 'eurohpcavail': 0.35, 'ndays': 0, 'armusedhours': 0, 'amdusedhours': 0,
        'gpuusedhours': 0, 'gpuusedhoursEuroHPC': 0, 'amdusedhoursEuroHPC': 0, 'armusedhoursEuroHPC': 0,
        'armJobs': 0, 'amdJobs': 0, 'gpuJobs': 0, 'gpuCompletedJobs': 0, 'gpuFailedJobs': 0,
        'armCompletedJobs': 0, 'amdCompletedJobs': 0, 'amdFailedJobs': 0, 'armFailedJobs': 0,
        'gpuJobsEuroHPC': 0, 'amdJobsEuroHPC': 0, 'armJobsEuroHPC': 0, 'ndaysTrimester': 0,
        'gpuCompletedJobsTrimester': 0, 'gpuFailedJobsTrimester': 0, 'armCompletedJobsTrimester': 0,
        'amdCompletedJobsTrimester': 0, 'amdFailedJobsTrimester': 0, 'armFailedJobsTrimester': 0,
        'armusedhoursTrimester': 0, 'amdusedhoursTrimester': 0, 'gpuusedhoursTrimester': 0,
        'armJobsTrimester': 0, 'amdJobsTrimester': 0, 'gpuJobsTrimester': 0,
        'gpuusedhoursEuroHPCTrimester': 0, 'amdusedhoursEuroHPCTrimester': 0, 'armusedhoursEuroHPCTrimester': 0,
        'gpuJobsEuroHPCTrimester': 0, 'amdJobsEuroHPCTrimester': 0, 'armJobsEuroHPCTrimester': 0,
        'ndaysYear': 0, 'gpuCompletedJobsYear': 0, 'gpuFailedJobsYear': 0, 'armCompletedJobsYear': 0,
        'amdCompletedJobsYear': 0, 'amdFailedJobsYear': 0, 'armFailedJobsYear': 0,
        'armusedhoursYear': 0, 'amdusedhoursYear': 0, 'gpuusedhoursYear': 0, 'armJobsYear': 0,
        'amdJobsYear': 0, 'gpuJobsYear': 0, 'gpuJobsEuroHPCYear': 0, 'amdJobsEuroHPCYear': 0,
        'armJobsEuroHPCYear': 0, 'gpuusedhoursEuroHPCYear': 0, 'amdusedhoursEuroHPCYear': 0,
        'armusedhoursEuroHPCYear': 0,
        'monthhours': '{\\inteval{\\ndays * 24}}',
        'hoursTrimester': '{\\inteval{\\ndaysTrimester * 24}}',
        'hoursYear': '{\\inteval{\\ndaysYear * 24}}'
    }

    list_of_Months = list(calendar.month_name)[1:]
    list_of_months_abr = list(calendar.month_abbr)[1:]

    today = datetime.now().date()
    year = today.year
    month_int = today.month - 2
    month = list_of_months_abr[month_int]
    syear = date(year, 1, 1)

    if args.month != None:
        month_int = list_of_months_abr.index(args.month)
        month = list_of_months_abr[month_int]

    if args.year != None:
        year = int(args.year)
        syear = date(year, 1, 1)

    if args.start != None:
        syear = datetime.strptime(args.start, "%Y-%m-%d").date()

    params['reportMonth'] = list_of_Months[month_int]
    params['reportYear'] = year

    smonth = date(year, month_int + 1, 1)
    emonthd = smonth + relativedelta(months=1) + relativedelta(days=-1)
    emonth = smonth + relativedelta(months=1)

    if month_int < 3:
        tmonth = emonth - relativedelta(months=month_int + 1)
    else:
        tmonth = emonth - relativedelta(months=3)

    params['reportPeriod'] = f"{smonth.strftime('%d/%m/%Y')} - {emonthd.strftime('%d/%m/%Y')}"
    if month_int < 3:
        params['reportPeriodTrimester'] = f"{syear.strftime('%d/%m/%Y')} - {emonthd.strftime('%d/%m/%Y')}"
    else:
        params['reportPeriodTrimester'] = f"{tmonth.strftime('%d/%m/%Y')} - {emonthd.strftime('%d/%m/%Y')}"

    params['reportPeriodYear'] = f"{syear.strftime('%d/%m/%Y')} - {emonthd.strftime('%d/%m/%Y')}"

    params['ndays'] = (emonth - smonth).days
    params['ndaysTrimester'] = (emonth - tmonth).days
    params['ndaysYear'] = (emonth - syear).days

    tag_month = {
        '': [month, ],
        'Trimester': None,
        'Year': list_of_months_abr[:month_int + 1]
    }

    if month_int < 3:
        tag_month['Trimester'] = list_of_months_abr[:month_int + 1]
    else:
        tag_month['Trimester'] = list_of_months_abr[month_int - 2:month_int + 1]

    outfilename = "params.tex"
    if args.outfile != None:
        outfilename = args.outfile

    os.makedirs(OUTDIR, exist_ok=True)  # Garante que a pasta existe
    wfile = open(f"{OUTDIR}/{outfilename}", "w+")

    # --- CONFIGURAÇÃO CORRIGIDA DOS LOGS ---
    sc = (SparkSession.builder
          .config("spark.eventLog.enabled", "true")  # [cite: 67]
          .config("executor.memory", "4g")
          .config("num.executors", "4")
          # <-- ATUALIZADO: Caminho real para os teus eventos
          .config("spark.eventLog.dir", f"file:///projects/F202500010HPCVLABUMINHO/uminhocp043/spark-events")
          .getOrCreate()
          )

    # LÓGICA ORIGINAL DO SCRIPT (Iteração ficheiro a ficheiro) - NÃO TOCAR PARA O BASELINE!
    nd = None
    for root, dirs, files in os.walk(DATADIR):
        for f in files:
            if f.startswith('jobs'):
                month_file = "_".join(f.split("_")[1:]).split(".")[0]
                data = sc.read.option("delimiter", "|").csv(f'{DATADIR}/{f}', inferSchema=True, header=True)
                data = data \
                    .withColumn('EState', F.regexp_replace(F.col('State'), "CANCELLED(.*)", "CANCELLED")) \
                    .withColumn('COMPLETED', F.when(F.col('State') == 'COMPLETED', "COMPLETED").otherwise("FAILED"))
                data = data.withColumn('Period', F.lit(month_file))
                if nd == None:
                    nd = data
                else:
                    nd = nd.union(data)

    tag = ""

    nd = nd.withColumn("cluster",
                       F.when(F.col('Partition').contains("arm"), "ARM")
                       .otherwise(F.when(F.col('Partition').contains("a100"), "GPU").otherwise("AMD"))
                       )

    nd = nd.withColumn("Agency",
                       F.when(F.col('Account').startswith("f"), "FCT")
                       .otherwise(F.when(F.col('Account').startswith("ee"), "EHPC").otherwise("LOCAL"))
                       )

    nd = nd.withColumn("OldVNodes", F.when(
        F.col("Partition").contains("a100"),
        F.when(F.col('AllocCPUS') % 32 == 0, (F.cast(int, F.col('AllocCPUS') / 32)))
        .otherwise((F.cast(int, F.col('AllocCPUS') / 32) + 1))
    ).otherwise(F.col("NNodes")))

    nd = nd.withColumn("VNodes", F.when(
        F.col("Partition").contains("a100"),
        F.when(F.col("AllocTRES").isNull(), F.col("NNodes"))
        .otherwise(
            F.when(F.col("AllocTRES").rlike(r"gres/gpu=(\d+)"),
                   F.regexp_extract(F.col("AllocTRES"), r"gres/gpu=(\d+)", 1)
                   ).otherwise(F.col("NNodes") * 4)
        )
    ).otherwise(F.col("NNodes"))
                       )

    nd = nd.withColumn("totalJobSeconds", (F.col('ElapsedRaw')) * F.col('VNodes'))

    # --- ADICIONADO: IMPRIMIR O PLANO DE EXECUÇÃO (OBRIGATÓRIO) ---
    print("\n\n--- PHYSICAL PLAN (BASELINE) ---")
    nd.explain("extended")  
    print("--------------------------------\n\n")

    cl = ['ARM', 'AMD', 'GPU']

    # LÓGICA ORIGINAL DE COMPUTAÇÃO - MANTÉM OS COLLECTS LENTOS PARA MEDIRMOS A DOR!
    for tag, months in tag_month.items():
        hours = dict()
        jobs = dict()
        completed = nd.filter(F.col('Period').isin(months)).groupby('COMPLETED', 'cluster').count().collect()

        for row in completed:
            if row.COMPLETED == 'COMPLETED':
                params[f"{row.cluster.lower()}CompletedJobs{tag}"] = row.asDict()['count']
            else:
                params[f"{row.cluster.lower()}FailedJobs{tag}"] = row.asDict()['count']

        for c in cl:
            hours_data = nd.filter(F.col("Agency") != 'LOCAL').filter(F.col('Period').isin(months)) \
                .groupby("cluster").sum().filter(F.col("cluster") == c).collect()
            hours[c] = hours_data[0].asDict()['sum(totalJobSeconds)'] if hours_data else 0

        for row in nd.filter(F.col("Agency") != 'LOCAL').filter(F.col('Period').isin(months)) \
                .groupby("cluster").count().collect():
            r = row.asDict()
            jobs[r['cluster']] = r['count']

        for k, v in hours.items():
            params[f"{k.lower()}usedhours{tag}"] = v / 3600
        for k, v in jobs.items():
            params[f"{k.lower()}Jobs{tag}"] = v

        for row in (nd.filter(F.col('Period').isin(months)).groupby(['Agency', 'cluster'])
                .count().orderBy('Agency').filter(F.col("Agency") == 'EHPC').collect()):
            params[f"{row.cluster.lower()}JobsEuroHPC{tag}"] = row.asDict()['count']

        rows = (nd.filter(F.col("Agency") == 'EHPC').filter(F.col('Period').isin(months))
                .groupby(['Agency', 'cluster']).sum().collect())

        for row in rows:
            params[f"{row.cluster.lower()}usedhoursEuroHPC{tag}"] = row.asDict()['sum(totalJobSeconds)'] / 3600

    for k, v in params.items():
        msg = f"\\def\\{k}{{{v}}}\n"
        wfile.write(msg)

    wfile.close()

    # --- FIM DA MEDIÇÃO DE TEMPO ---
    t_end = time.time()
    print(f"\n[METRICS] Total Wall-Clock Runtime: {t_end - t_start:.2f} seconds")  # [cite: 73]