# Registo de Otimizações Spark SQL (Track A)

Este documento detalha o progresso das otimizações individuais aplicadas ao script de extração de dados do HPC, conforme a ordem exigida no guião do projeto. Todas as métricas apresentadas representam as médias de 3 execuções independentes, registadas após o aquecimento prévio da JVM.

##  Baseline
O script de base processa os dados de forma ineficiente. A análise do plano físico e do tempo de execução revelou falhas graves:
* **Leitura Iterativa:** Usa `os.walk` para ler ficheiros um a um com dezenas de `.union()`.
* **Driver Bottleneck:** Muitas ações `.collect()` dentro de loops obrigam o Spark a parar o processamento e a trocar dados com o *Driver*.
* **Métricas de Referência:** 76.57s Runtime | 77 Stages | 135 Tasks | 14.22s Driver Time.

---

##  Otimização 1: Caching / Persisting
**O que mudou:** Impedimos que o Spark relesse os ficheiros do disco dezenas de vezes durante os loops de escrita.
**Como fizemos:** Injetámos o comando `nd.persist(pyspark.StorageLevel.MEMORY_AND_DISK)` e forçámos uma ação imediata (`.count()`) para materializar o DataFrame na memória RAM dos executores logo após a fase de transformação inicial.

### 📊 Resultados Isolados (Opt 1)
| Métrica | Média (3 runs) |
| :--- | :--- |
| **Runtime (Wall-Clock)** | 67.29 sec |
| **# Stages** | 77 |
| **# Tasks** | 135 |
| **Shuffle Read (MB)** | 0.0000 MB |
| **Shuffle Write (MB)** | 0.0176 MB |
| **Driver Time (sec)** | 15.54 sec |

*Análise Breve:* Como a estrutura lógica do plano físico não mudou (os loops e os `unions` mantiveram-se), o número de Stages e Tasks permaneceu idêntico (77 e 135, respetivamente). No entanto, o facto de o Spark ler a partir da memória e não do disco reduziu o tempo total de execução em quase 10 segundos.

---

##  Otimização 2: Repartitioning / Coalescing
**O que mudou:** O Baseline criava um DataFrame altamente fragmentado e desequilibrado devido aos múltiplos `.union()` na fase de leitura. Rebalanceámos a carga de trabalho.
**Como fizemos:** Introduzimos um comando `.repartition()` antes das ações de agregação para forçar o Spark a baralhar os dados e a criar partições de tamanho igual.

### 📊 Resultados Isolados (Opt 2)
| Métrica | Média (3 runs) |
| :--- | :--- |
| **Runtime (Wall-Clock)** | 72.93 sec |
| **# Stages** | 79 |
| **# Tasks** | 327 |
| **Shuffle Read (MB)** | 0.0000 MB |
| **Shuffle Write (MB)** | 0.0811 MB |
| **Driver Time (sec)** | 13.56 sec |

*Análise Breve:* O reparticionamento obriga a uma troca de dados na rede, o que justifica o aumento visível do *Shuffle Write* para 0.0811 MB e o aumento das *Tasks* para 327. O tempo total melhorou ligeiramente face ao Baseline (72.93s), pois as partições ficaram mais equilibradas para as fases seguintes.

---

##  Otimização 3: Join / Shuffle Reduction
**O que mudou:** Reduzimos a quantidade de operações executadas repetidamente dentro de loops para calcular métricas de tempo e agência.
**Como fizemos:** Aplicámos uma *Single-Pass Conditional Aggregation*. O dataset foi agrupado por cluster de uma só vez, e condições como período, estado e agência foram avaliadas via funções condicionais (`CASE WHEN`) num único comando `.agg()`.

### 📊 Resultados Isolados (Opt 3)
| Métrica | Média (3 runs) |
| :--- | :--- |
| **Runtime (Wall-Clock)** | 55.63 sec |
| **# Stages** | 31 |
| **# Tasks** | 112 |
| **Shuffle Read (MB)** | 0.0000 MB |
| **Shuffle Write (MB)** | 0.0171 MB |
| **Driver Time (sec)** | 14.45 sec |

*Análise Breve:* A remoção das filtragens em loop reduziu massivamente as *Stages* necessárias para 31, originando uma descida do Wall-Clock para ~55 segundos.

---

##  Otimização 4: Query Simplification (Parallel Read)
**O que mudou:** Removemos o loop iterativo do Python que lia os ficheiros do disco um a um de forma sequencial.
**Como fizemos:** Substituímos o iterativo `.union()` por uma leitura paralela com um *wildcard path* (`sc.read.csv("jobs_*.txt")`). O mês de cada registo passou a ser extraído de forma distribuída nativa através da função `input_file_name()`.

###  Resultados Isolados (Opt 4)
| Métrica | Média (3 runs) |
| :--- | :--- |
| **Runtime (Wall-Clock)** | 53.47 sec |
| **# Stages** | 50 |
| **# Tasks** | 556 |
| **Shuffle Read (MB)** | 0.0000 MB |
| **Shuffle Write (MB)** | 0.0055 MB |
| **Driver Time (sec)** | 11.98 sec |

*Análise Breve:* A ingestão de ficheiros passou a ser totalmente paralela. O elevado número de *Tasks* (556) espelha a forma como o Spark divide ativamente a leitura massiva inicial pelos executores disponíveis, sem o bloqueio do *driver*.

---

##  Otimização 5: Output Path Efficiency (Single Collect)
**O que mudou:** Evitámos dezenas de chamadas `.collect()` consecutivas que estrangulavam a comunicação no nó *Driver* durante a escrita final do `.tex`.
**Como fizemos:** Agregámos todos os dados do DataFrame de uma vez só pelas chaves principais (`Period`, `Agency`, `cluster`, `COMPLETED`). Efetuámos um único `.collect()`, trazendo um dicionário agregado e minúsculo para a RAM do Python, de onde o ficheiro `.tex` foi escrito de rajada (*single pass*).

### 📊 Resultados Isolados (Opt 5)
| Métrica | Média (3 runs) |
| :--- | :--- |
| **Runtime (Wall-Clock)** | 49.04 sec |
| **# Stages** | 26 |
| **# Tasks** | 61 |
| **Shuffle Read (MB)** | 0.0000 MB |
| **Shuffle Write (MB)** | 0.0473 MB |
| **Driver Time (sec)** | 14.39 sec |

*Análise Breve:* Otimização massiva e a mais rápida de todas as intervenções individuais. Baixámos o tempo para ~49 segundos com um esforço mínimo de apenas 26 *Stages* e 61 *Tasks*. O ligeiro aumento no *Shuffle Write* (0.0473 MB) é natural, derivando do agrupamento multidimensional simultâneo antes do retorno ao mestre.
