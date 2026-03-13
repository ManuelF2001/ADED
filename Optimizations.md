# Registo de Otimizações Spark SQL (Track A)

Este documento detalha o progresso das otimizações individuais aplicadas ao script de extração de dados do HPC, conforme exigido no guião do projeto.

## 🐢 Baseline
O script de base (`baseline.py`) processa os dados de forma muito ineficiente. A análise do plano físico (`explain("extended")`) e do tempo de execução revelou falhas graves:
* **Leitura Iterativa:** Usa um ciclo `for` em Python (`os.walk`) para ler os ficheiros um a um e efetua dezenas de operações `.union()`. Isto impede o Spark de planear a leitura em paralelo.
* **Driver Bottleneck:** Possui dezenas de ações `.collect()` dentro de loops (por período, por agência, etc.). Isto obriga o Spark a parar o processamento distribuído constantemente para enviar pequenos pedaços de dados de volta ao *Driver*.

---

## ⚡ Otimização 1: Query Simplification (Parallel Read)
**O que mudou:** Removemos o loop iterativo do Python que lia ficheiros individualmente.
[cite_start]**Como fizemos:** Substituímos o bloco `os.walk` e o iterativo `.union()` por uma leitura única e paralela usando um *wildcard path* (`sc.read.csv("jobs_*.txt")`). O mês de cada registo passou a ser extraído de forma distribuída diretamente da string do nome do ficheiro usando a função `pyspark.sql.functions.input_file_name()` e expressões regulares.
**Impacto Esperado:** Redução massiva do estrangulamento na leitura e paralelização nativa do Dataframe logo na ingestão.

---

## ⚡ Otimização 5: Output Path Efficiency (Single Collect)
**O que mudou:** Evitámos múltiplas chamadas `.collect()` e o trabalho sequencial de geração do ficheiro de output[cite: 48, 130].
**Como fizemos:** Em vez de usar filtros e operações `groupby` repetidas dentro de um ciclo temporal, agregámos todos os dados do DataFrame de uma só vez usando um único `groupby` pelas chaves principais (`Period`, `Agency`, `cluster`, `COMPLETED`).
Efetuámos um único `.collect()` deste resultado agregado e pré-calculado (que é muito pequeno e cabe facilmente na RAM do Python). O ficheiro `params.tex` é assim escrito numa passagem única (*single pass*) a partir deste dicionário processado.

### 📊 Resultados Isolados (Opt 5)
Abaixo encontram-se as métricas recolhidas no HPC Deucalion, usando o nosso *Event-log parser*[cite: 83]. [cite_start]Os resultados representam a média e o desvio padrão de 3 execuções válidas (após "warm-up" da JVM).

| Métrica | Média | Desvio Padrão |
| :--- | :--- | :--- |
| **Runtime (Wall-Clock)** | 49.0433 sec | 0.4456 |
| **# Stages** | 26.0000 | 0.0000 |
| **# Tasks** | 61.0000 | 0.0000 |
| **Shuffle Read (MB)** | 0.0000 MB | 0.0000 |
| **Shuffle Write (MB)** | 0.0473 MB | 0.0000 |
| **Driver Time (sec)** | 14.3850 sec | 0.2311 |

*Análise Breve:* A redução do número de *Stages* e *Tasks* face à Baseline é notória. Contudo, introduzimos um ligeiro custo de *Shuffle Write* (0.0473 MB), perfeitamente aceitável e justificado pelo facto de o Spark ter agora de trocar dados entre partições na rede para efetuar o grande agrupamento massivo antes do `.collect()` final.
