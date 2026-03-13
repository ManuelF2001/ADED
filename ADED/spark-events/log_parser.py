import json
import sys
import os


def parse_spark_event_log(log_path):
    metrics = {
        "stages_submitted": 0,
        "stages_completed": 0,
        "total_tasks": 0,
        "shuffle_read_kb": 0.0,
        "shuffle_write_kb": 0.0,
        "min_task_start": float('inf'),
        "max_task_end": 0.0
    }

    if not os.path.exists(log_path):
        print(f"Erro: Ficheiro não encontrado: {log_path}")
        return

    with open(log_path, 'r') as f:
        for line in f:
            try:
                event = json.loads(line.strip())
            except json.JSONDecodeError:
                continue

            event_type = event.get("Event")

            if event_type == "SparkListenerStageSubmitted":
                metrics["stages_submitted"] += 1
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

                # Shuffle Read (Convertido para KB)
                shuffle_read = task_metrics.get("Shuffle Read Metrics", {})
                read_bytes = shuffle_read.get("Total Bytes Read", 0)
                metrics["shuffle_read_kb"] += read_bytes / 1024

                # Shuffle Write (Convertido para KB)
                shuffle_write = task_metrics.get("Shuffle Write Metrics", {})
                write_bytes = shuffle_write.get("Shuffle Bytes Written", 0)
                metrics["shuffle_write_kb"] += write_bytes / 1024

    print(f"--- Métricas para {os.path.basename(log_path)} ---")
    print(f"Stages Submitted:  {metrics['stages_submitted']}")
    print(f"Stages Completed:  {metrics['stages_completed']}")
    print(f"Total Tasks:       {metrics['total_tasks']}")
    print(f"Shuffle Read (KB): {metrics['shuffle_read_kb']:.2f}")
    print(f"Shuffle Write (KB):{metrics['shuffle_write_kb']:.2f}")

    if metrics['max_task_end'] > 0:
        task_time_seconds = (metrics['max_task_end'] - metrics['min_task_start']) / 1000
        print(f"Tempo total das Tasks (sec): {task_time_seconds:.2f}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python log_parser.py <caminho_para_o_ficheiro_spark_event_log>")
    else:
        parse_spark_event_log(sys.argv[1])