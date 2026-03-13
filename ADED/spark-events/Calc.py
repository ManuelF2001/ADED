import json
import sys
import os
import statistics


def parse_spark_event_log(log_path):
    metrics = {
        "stages_completed": 0,
        "total_tasks": 0,
        "shuffle_read_mb": 0.0,
        "shuffle_write_mb": 0.0,
        "min_task_start": float('inf'),
        "max_task_end": 0.0
    }

    if not os.path.exists(log_path):
        print(f"Erro: Ficheiro não encontrado: {log_path}")
        return None

    with open(log_path, 'r') as f:
        for line in f:
            try:
                event = json.loads(line.strip())
            except json.JSONDecodeError:
                continue

            event_type = event.get("Event")

            if event_type == "SparkListenerStageSubmitted":
                stage_info = event.get("Stage Info", {})
                metrics["total_tasks"] += stage_info.get("Number of Tasks", 0)

            elif event_type == "SparkListenerStageCompleted":
                metrics["stages_completed"] += 1

            elif event_type == "SparkListenerTaskEnd":
                task_info = event.get("Task Info", {})
                start_time = task_info.get("Launch Time", float('inf'))
                end_time = task_info.get("Finish Time", 0.0)

                metrics["min_task_start"] = min(metrics["min_task_start"], start_time)
                metrics["max_task_end"] = max(metrics["max_task_end"], end_time)

                task_metrics = event.get("Task Metrics", {})

                # Shuffle (Convertido para MB com precisão, conforme guião)
                shuffle_read = task_metrics.get("Shuffle Read Metrics", {})
                metrics["shuffle_read_mb"] += shuffle_read.get("Total Bytes Read", 0) / (1024 * 1024)

                shuffle_write = task_metrics.get("Shuffle Write Metrics", {})
                metrics["shuffle_write_mb"] += shuffle_write.get("Shuffle Bytes Written", 0) / (1024 * 1024)

    # Calcular o tempo total das tasks
    if metrics['max_task_end'] > 0:
        metrics['task_span_sec'] = (metrics['max_task_end'] - metrics['min_task_start']) / 1000
    else:
        metrics['task_span_sec'] = 0.0

    return metrics


def format_output(name, values):
    mean = statistics.mean(values)
    stdev = statistics.stdev(values) if len(values) > 1 else 0.0
    # Imprime com alinhamento para ser fácil de copiar para a tabela do relatório
    print(f"{name:<20} | Média: {mean:>10.4f} | Desvio Padrão: {stdev:>10.4f}")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Uso correto: python3 calc_metrics.py <log_run_1> <log_run_2> <log_run_3>")
        sys.exit(1)

    log_files = [sys.argv[1], sys.argv[2], sys.argv[3]]
    parsed_data = []

    print("\n--- A ler os 3 ficheiros de log... ---")
    for lf in log_files:
        data = parse_spark_event_log(lf)
        if data:
            parsed_data.append(data)
        else:
            sys.exit(1)

    # Pedir ao utilizador os tempos Wall-Clock que apareceram no terminal (.out)
    print("\nPor favor, insere o 'Total Wall-Clock Runtime' (em segundos) que apareceu no ficheiro .out para cada run.")
    wall_clocks = []
    for i in range(3):
        while True:
            try:
                val = float(input(f"Wall-Clock Runtime para a Run {i + 1} ({os.path.basename(log_files[i])}): "))
                wall_clocks.append(val)
                break
            except ValueError:
                print("Por favor, insere um número válido (ex: 73.12).")

    # Extrair listas de métricas para calcular médias e desvios padrão
    stages = [d['stages_completed'] for d in parsed_data]
    tasks = [d['total_tasks'] for d in parsed_data]
    shuffle_r = [d['shuffle_read_mb'] for d in parsed_data]
    shuffle_w = [d['shuffle_write_mb'] for d in parsed_data]

    # Calcular o Driver Time para cada run: Wall-Clock - Task Span
    driver_times = []
    for i in range(3):
        d_time = wall_clocks[i] - parsed_data[i]['task_span_sec']
        # Evitar valores negativos caso haja imprecisões milimétricas
        driver_times.append(max(0.0, d_time))

    print("\n===========================================================")
    print("      RESULTADOS FINAIS PARA A TABELA DO RELATÓRIO         ")
    print("===========================================================")
    format_output("Runtime (Wall-Clock)", wall_clocks)
    format_output("# Stages", stages)
    format_output("# Tasks", tasks)
    format_output("Shuffle Read (MB)", shuffle_r)
    format_output("Shuffle Write (MB)", shuffle_w)
    format_output("Driver Time (sec)", driver_times)
    print("===========================================================\n")