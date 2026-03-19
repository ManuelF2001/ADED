# Big Data Analysis - HPC Job Analysis & Spark SQL Optimization

Este repositório contém o código e os resultados do Projeto Prático da unidade curricular de Big Data Analysis da Universidade do Minho.

## 📌 Sobre o Projeto
O objetivo deste trabalho é processar logs do supercomputador Deucalion para gerar um relatório trimestral detalhado sobre a utilização dos recursos. O sistema reporta métricas como o número de *jobs* concluídos e falhados por partição (ARM, AMD, GPU), convertendo os resultados num ficheiro LaTeX (`params.tex`).

Foi-nos fornecido um script inicial (`statsEHPC_v2_init.py`) que já produz o relatório, mas de forma ineficiente. O foco do nosso trabalho incide na **Track A: Spark SQL Optimization**.

##  Metodologia & Benchmarking Protocol
Para garantir o rigor científico e a reprodutibilidade, o projeto foi executado sob uma metodologia estrita no supercomputador Deucalion (SLURM, partição `normal-arm`, com alocação de 3 nós dedicados).

O nosso processo de Benchmarking incluiu:
* **Isolamento de Otimizações:** As otimizações (Leitura Paralela, Caching, Repartitioning, Single-Pass Aggregation, etc.) foram implementadas e testadas num script próprio de cada vez, garantindo que o ficheiro final gerado (`params.tex`) mantinha a exatidão semântica.
* **Warm-up da JVM:** Antes de registar os dados de qualquer configuração, foi sempre feita uma execução inicial de aquecimento ("Warmup") que foi prontamente descartada, de forma a aquecer a *Java Virtual Machine* e as caches do sistema.
* **Tripla Execução:** Após o *warmup*, cada script foi submetido e executado 3 vezes consecutivas. Os resultados apresentados nos nossos relatórios são a média matemática dessas 3 execuções.
* **Recolha de Métricas Não-Intrusiva:** Em vez de usar *profilers* pesados, ativámos a gravação de Event Logs nativos do Spark em JSON. Usámos um parser Python desenvolvido por nós para extrair as Stages, Tasks e volumes de Shuffle diretamente desses logs.

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
├── Opt3_Warmup_File/          # Otimização 3: Join/Shuffle Reduction
│   └── Opt3.py                # Agregação condicional numa única passagem (Single-Pass)
├── Opt5_Warmup_File/          # Otimização 5: Output Path Efficiency
│   └── Opt5.py                # Script usando agregação única em vez de iterativa
├── Opt5_Results/              # Pasta modelo com as execuções válidas para cálculo de métricas
│   ├── Opt5_Run1.out / Run2.out / Run3.out  # Logs de standard output
│   ├── app-1 / app-2 / app-3                # Event Logs do Spark em JSON
│   └── Resultados.txt                       # Tabela final com Médias e Desvios Padrão
├── script_ADED.sh             # Script Slurm de alocação de nós e arranque do cluster
└── spark-events/              # Diretório principal de logs do Spark e ferramentas
    ├── log_parser.py          # Script de parsing individual de Event Logs (Stages, Tasks, Shuffle)
    └── Calc.py                # Script agregador que processa 3 runs e gera a tabela final
