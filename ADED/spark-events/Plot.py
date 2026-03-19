import matplotlib.pyplot as plt
import sys
import os


def parse_vmstat(file_path):
    """Lê um ficheiro vmstat e extrai a Memória Livre (MB), Context Switches e CPU User."""
    data = []
    if not os.path.exists(file_path):
        print(f"Aviso: Ficheiro {file_path} não encontrado. Será ignorado.")
        return data

    with open(file_path, 'r') as f:
        for line in f:
            parts = line.strip().split()
            # Ignorar linhas vazias ou cabeçalhos (procurar apenas linhas de números)
            if len(parts) > 12 and parts[0].isdigit():
                try:
                    # Colunas do vmstat: 3 = free mem (KB), 11 = cs, 12 = us (CPU)
                    free_mem_mb = int(parts[3]) / 1024
                    cs = int(parts[11])
                    cpu = int(parts[12])

                    data.append((free_mem_mb, cs, cpu))
                except ValueError:
                    continue
    return data


def plot_averaged_vmstat(files):
    all_data = []

    print("A processar ficheiros...")
    for f in files:
        d = parse_vmstat(f)
        if d:
            all_data.append(d)
            print(f" - {f}: {len(d)} segundos de dados carregados.")

    if len(all_data) == 0:
        print("\nErro: Nenhum dado válido encontrado para gerar o gráfico.")
        return

    # Para fazer a média perfeitamente, limitamos o tempo à run mais curta
    min_length = min(len(d) for d in all_data)
    print(f"\nA alinhar e calcular médias para {min_length} segundos...")

    time_sec = []
    avg_free_mem = []
    avg_cs = []
    avg_cpu = []

    # Calcular a média de cada segundo
    for i in range(min_length):
        time_sec.append(i)

        free_mem_val = sum(run[i][0] for run in all_data) / len(all_data)
        cs_val = sum(run[i][1] for run in all_data) / len(all_data)
        cpu_val = sum(run[i][2] for run in all_data) / len(all_data)

        avg_free_mem.append(free_mem_val)
        avg_cs.append(cs_val)
        avg_cpu.append(cpu_val)

    # Criar a figura com 3 subgráficos partilhando o eixo do tempo (X)
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(10, 8), sharex=True)
    fig.suptitle(f'Análise de Recursos do Driver (Média de {len(all_data)} Execuções)', fontsize=15, fontweight='bold')

    # Gráfico 1: CPU Usage (User)
    ax1.plot(time_sec, avg_cpu, color='tab:red', marker='o', linestyle='-', markersize=3, linewidth=1.5)
    ax1.set_ylabel('CPU User (%)')
    ax1.set_title('Média de Picos de Processamento (CPU)', fontsize=11)
    ax1.grid(True, linestyle='--', alpha=0.7)

    # Gráfico 2: Context Switches
    ax2.plot(time_sec, avg_cs, color='tab:orange', marker='s', linestyle='-', markersize=3, linewidth=1.5)
    ax2.set_ylabel('Context Switches / s')
    ax2.set_title('Média de Mudanças de Contexto (Network/IO)', fontsize=11)
    ax2.grid(True, linestyle='--', alpha=0.7)

    # Gráfico 3: Free Memory
    ax3.plot(time_sec, avg_free_mem, color='tab:blue', marker='^', linestyle='-', markersize=3, linewidth=1.5)
    ax3.set_xlabel('Tempo (segundos)', fontsize=12)
    ax3.set_ylabel('Memória Livre (MB)')
    ax3.set_title('Média do Consumo de RAM', fontsize=11)
    ax3.grid(True, linestyle='--', alpha=0.7)

    # Ajustar margens
    plt.tight_layout()

    # Guardar gráfico
    output_file = 'vmstat_analise_media.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\nSucesso! Gráfico guardado como '{output_file}'")


if __name__ == '__main__':
    # Se o utilizador chamar "python plot_vmstat.py ficheiro1 ficheiro2 ficheiro3"
    if len(sys.argv) > 1:
        target_files = sys.argv[1:]
    else:
        # Ficheiros por defeito se nenhum argumento for passado
        target_files = ['vmstat_run1.txt', 'vmstat_run2.txt', 'vmstat_run3.txt']

    plot_averaged_vmstat(target_files)