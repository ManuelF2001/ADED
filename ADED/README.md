# Registo de Otimizações Spark SQL (Track A)

Este documento detalha o progresso das otimizações individuais aplicadas ao script de extração de dados do HPC, conforme exigido no guião do projeto. Todas as métricas apresentadas representam as médias de 3 execuções independentes, registadas após o aquecimento prévio da JVM.

## 🐢 Baseline
O script de base (`baseline.py`) processa os dados de forma ineficiente. A análise do plano físico e do tempo de execução revelou falhas:
* **Leitura Iterativa:** Usa `os.walk` para ler ficheiros um a um com dezenas de `.union()`, impedindo paralelização nativa.
* **Driver Bottleneck:** Muitas ações `.collect()` dentro de loops obrigam o Spark a parar o processamento e a trocar dados com a memória restrita do *Driver*.
* **Runtime de Referência:** ~76.57 segundos | 77 Stages | 135 Tasks.

---

## ⚡ Otimização 4: Query Simplification (Parallel Read)
**O que mudou:** Removemos o loop iterativo do Python que lia ficheiros individualmente.
**Como fizemos:** Substituímos o iterativo `.union()` por uma leitura paralela com *wildcard path* (`sc.read.csv("jobs_*.txt")`). O mês de cada registo passou a ser extraído através da função `pyspark.sql.functions.input_file_name()`.

### 📊 Resultados Isolados (Opt 1)
| Métrica | Média (3 runs) | Desvio Padrão |
| :--- | :--- | :--- |
| **Runtime (Wall-Clock)** | 53.4700 sec | 0.7212 |
| **# Stages** | 50.0000 | 0.0000 |
| **# Tasks** | 556.0000 | 0.0000 |
| **Shuffle Read (MB)** | 0.0000 MB | 0.0000 |
| **Shuffle Write (MB)** | 0.0055 MB | 0.0000 |
| **Driver Time (sec)** | 11.9790 sec | 0.1457 |

*Análise Breve:* A ingestão de ficheiros passou a ser distribuída. Os tempos de execução baixaram >23s. O número disparado de *Tasks* (556) reflete o facto de o Spark dividir ativamente a leitura massiva pelos executores.

---

## ⚡ Otimização 3: Join / Shuffle Reduction
**O que mudou:** Reduzimos a quantidade de operações executadas dentro de loops para calcular métricas de tempo e agência.
**Como fizemos:** Aplicámos uma *Single-Pass Conditional Aggregation*. O dataset foi agrupado de uma só vez. Condições como período, estado e agência foram avaliadas via `pyspark.sql.functions.when` num único comando `.agg()`.

### 📊 Resultados Isolados (Opt 3)
| Métrica | Média (3 runs) | Desvio Padrão |
| :--- | :--- | :--- |
| **Runtime (Wall-Clock)** | 55.6300 sec | -- |
| **# Stages** | 31.0000 | -- |
| **# Tasks** | 112.0000 | -- |
| **Shuffle Read (MB)** | 0.0000 MB | -- |
| **Shuffle Write (MB)** | 0.0171 MB | -- |
| **Driver Time (sec)** | 14.4500 sec | -- |

*Análise Breve:* Remover as contagens dentro de um loop poupou cerca de 46 *Stages* ao motor Catalyst do Spark, baixando o runtime global significativamente face ao Baseline.

---

## ⚡ Otimização 5: Output Path Efficiency (Single Collect)
**O que mudou:** Evitámos dezenas de chamadas `.collect()` consecutivas que estrangulavam a escrita final do `.tex`.
**Como fizemos:** Agregámos todos os dados do DataFrame pelas chaves principais (`Period`, `Agency`, `cluster`, `COMPLETED`). O Spark enviou depois um dicionário agregado muito reduzido numa única operação de rede (`single collect`), permitindo que a impressão das variáveis se fizesse imediatamente na RAM do driver.

### 📊 Resultados Isolados (Opt 5)
| Métrica | Média (3 runs) | Desvio Padrão |
| :--- | :--- | :--- |
| **Runtime (Wall-Clock)** | 49.0433 sec | 0.4456 |
| **# Stages** | 26.0000 | 0.0000 |
| **# Tasks** | 61.0000 | 0.0000 |
| **Shuffle Read (MB)** | 0.0000 MB | 0.0000 |
| **Shuffle Write (MB)** | 0.0473 MB | 0.0000 |
| **Driver Time (sec)** | 14.3850 sec | 0.2311 |

*Análise Breve:* Otimização massiva. Baixámos o tempo Wall-Clock para ~49 segundos com um esforço mínimo de apenas 26 *Stages*. O ligeiro custo acrescido no Shuffle Write é compensado pela eficiência de devolvermos dados pré-mastigados ao nó mestre.
