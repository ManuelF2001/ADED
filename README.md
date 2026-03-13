# Big Data Analysis - HPC Job Analysis & Spark SQL Optimization

Este repositório contém o código e os resultados do Projeto Prático da unidade curricular de Big Data Analysis da Universidade do Minho.

## 📌 Sobre o Projeto
O objetivo deste trabalho é processar logs do supercomputador Deucalion para gerar um relatório trimestral detalhado sobre a utilização dos recursos. O sistema reporta métricas como o número de *jobs* concluídos e falhados por partição (ARM, AMD, GPU), convertendo os resultados num ficheiro LaTeX (`params.tex`).

Foi-nos fornecido um script inicial (`statsEHPC_v2_init.py`) que já produz o relatório, mas de forma ineficiente. O foco do nosso trabalho incide na **Track A: Spark SQL Optimization**.

## 🚀 Metodologia & Benchmarking Protocol
O projeto consiste em identificar estrangulamentos no código de base e aplicar técnicas de otimização em Apache Spark de forma isolada e sistemática:
* **Uma alteração de cada vez:** As otimizações são testadas individualmente para garantir que o ficheiro final gerado (`params.tex`) mantém a exatidão semântica.
* **Warm-up da JVM:** Antes de registar os dados de uma configuração, é feita uma execução inicial ("Warmup") que é descartada, de forma a aquecer a *Java Virtual Machine*.
* **Tripla Execução:** Após o *warmup*, cada configuração é executada 3 vezes. Os resultados finais reportam a média e o desvio padrão.
* **Métricas Extraídas:** Runtime (wall-clock), Driver Time, e contagem de Stages, Tasks e Shuffle Read/Write (via parsing dos Event Logs JSON gerados nativamente pelo Spark).

## 📂 Estrutura do Repositório

Adotou-se uma estrutura hierárquica baseada no ciclo de testes, separando claramente os ficheiros de aquecimento (*warmup*) dos testes definitivos (*results*).

```text
.
├── Baseline_Warmup_File/      # Testes de aquecimento do script original (lento)
│   ├── Baseline.py            # Script original instrumentado com cronómetros
│   └── Baseline.out           # Output do terminal com Runtime e Physical Plan
├── Opt1_Warmup_File/          # Otimização 1: Query Simplification (Parallel Read)
│   ├── Opt1.py                # Script substituindo iteradores por wildcard path
│   └── Opt1.out
├── Opt2_Warmup_File/          # Otimização 2: Caching / Persisting
│   └── Opt2.py                # Script forçando o armazenamento em MEMORY_AND_DISK
├── Opt5_Warmup_File/          # Otimização 5: Output Path Efficiency
│   └── Opt5.py                # Script usando agregação única em vez de iterativa
├── Opt5_Results/              # Pasta com as 3 execuções válidas para cálculo de métricas
│   ├── Opt5_Run1.out / Run2.out / Run3.out  # Logs de standard output
│   ├── app-1 / app-2 / app-3                # Event Logs do Spark em JSON
│   └── Resultados.txt                       # Tabela final com Médias e Desvios Padrão
├── script_ADED.sh             # Script Slurm de alocação de nós e arranque do cluster
└── spark-events/              # Diretório principal de logs do Spark e ferramentas
    ├── log_parser.py          # Script de parsing individual de Event Logs (Stages, Tasks, Shuffle)
    └── Calc.py                # Script agregador que processa 3 runs e gera a tabela final
