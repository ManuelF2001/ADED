# Big Data Analysis - HPC Job Analysis & Spark SQL Optimization

Este repositório contém o código e os resultados do Projeto Prático da unidade curricular de Big Data Analysis da Universidade do Minho.

## 📌 Sobre o Projeto
O objetivo deste trabalho é processar logs do supercomputador Deucalion para gerar um relatório trimestral detalhado sobre a utilização dos recursos. O sistema reporta métricas como o número de *jobs* concluídos e falhados por partição (ARM, AMD, GPU), convertendo os resultados num ficheiro LaTeX (`params.tex`).

Foi-nos fornecido um script inicial (`statsEHPC_v2_init.py`) que já produz o relatório, mas de forma ineficiente. O foco do nosso trabalho incide na **Track A: Spark SQL Optimization**.

## 🚀 Metodologia & Benchmarking Protocol
Para garantir o rigor científico e a reprodutibilidade, o projeto foi executado sob uma metodologia estrita no supercomputador Deucalion (SLURM, partição `normal-arm`, com alocação de 3 nós dedicados).

O nosso processo de Benchmarking incluiu:
* **Isolamento de Otimizações:** As otimizações foram implementadas e testadas num script próprio de cada vez, garantindo que o ficheiro final gerado (`params.tex`) mantinha a exatidão semântica.
* **Variação Paramétrica:** Em otimizações como o *Repartitioning*, foram testados múltiplos valores (ex: 32, 64, 144 partições) para encontrar o ponto ideal de equilíbrio da carga no cluster.
* **Warm-up da JVM:** Antes de registar os dados de qualquer configuração, foi sempre feita uma execução inicial de aquecimento ("Warmup") que foi prontamente descartada, de forma a aquecer a *Java Virtual Machine* e as caches do sistema.
* **Tripla Execução:** Após o *warmup*, cada script foi submetido e executado 3 vezes consecutivas. Os resultados apresentados nos nossos relatórios são a média matemática dessas 3 execuções.
* **Recolha de Métricas Não-Intrusiva:** Ativámos a gravação de Event Logs nativos do Spark em JSON. Usámos um parser Python próprio para extrair as Stages, Tasks e volumes de Shuffle diretamente desses logs.

## 📂 Estrutura do Repositório
Adotou-se uma estrutura hierárquica baseada no ciclo de testes, separando claramente os ficheiros de aquecimento (*warmup*) dos testes definitivos e segmentando os ensaios paramétricos.

```text
.
├── Baseline_Warmup_File/      # Teste inicial (descartado) do script original (lento)
│   ├── Baseline.py
│   └── Baseline.out
├── Baseline_Results/          # As 3 execuções válidas do Baseline e respetivos logs JSON
│   ├── app-20260319023038-0001 / ...0002 / ...0003
│   └── slurm-1050478.out
├── Opt1_Results/              # Resultados da Otimização 1 (Caching / Persisting)
│   ├── Opt1.py
│   ├── Opt1_Run1 / Opt1_Run2 / Opt1_Run3
│   ├── Opt1_Runs.out
│   └── Results.txt
├── Opt2_Rep32/                # Otimização 2 (Repartitioning) testada com 32 partições
│   ├── Opt2_32.py
│   ├── app-20260319043028-0001 / ...0002 / ...0003
│   └── slurm-1050700.out
├── Opt2_Rep64/                # Otimização 2 (Repartitioning) testada com 64 partições
│   ├── Opt2_64.py
│   ├── app-20260319041929-0001 / ...0002 / ...0003
│   └── slurm-1050693.out
├── Opt2_Rep144/               # Otimização 2 (Repartitioning) testada com 144 partições
│   ├── Opt2_144.py
│   ├── app-20260319034201-0001 / ...0002 / ...0003
│   └── slurm-1050662.out
├── Opt3_Warmup_File/          # Otimização 3 (Join / Shuffle Reduction) - Fase de Warmup
│   ├── Opt3.py
│   └── Opt3.out
├── Opt4_Warmup_File/          # Otimização 4 (Parallel Read) - Fase de Warmup
│   ├── Opt1.py                # (Ficheiro script Opt4 derivado da pipeline)
│   └── Opt1.out
├── Opt4_Results/              # Resultados da Otimização 4
│   ├── Opt1_Run1 / Opt1_Run2 / Opt1_Run3
│   ├── Opt1_Run1.out / Opt1_Run2.out / Opt1_Run3.out
│   └── Results.txt
├── Opt5_Warmup_File/          # Otimização 5 (Single Collect) - Fase de Warmup
│   ├── Opt5.py
│   └── Opt5.out
├── Opt5_Results/              # Resultados da Otimização 5
│   ├── app-1 / app-2 / app-3
│   ├── Opt5_Run1.out / Opt5_Run2.out / Opt5_Run3.out
│   └── Resultados.txt
├── script_ADED.sh             # Script Slurm principal de alocação e arranque
└── spark-events/              # Scripts auxiliares para parsing de logs do Spark
    ├── Calc.py
    └── log_parser.py
