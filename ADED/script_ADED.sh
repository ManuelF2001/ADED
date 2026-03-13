#!/bin/bash
#SBATCH --job-name spark-cluster
#SBATCH --nodes=3
#SBATCH --cpus-per-task=48
#SBATCH --mem=28G
#SBATCH --time=00:55:00
#SBATCH --account=F202500010HPCVLABUMINHOa
#SBATCH --partition=normal-arm

# 1. Carregar Módulos
source /share/env/module_select.sh
module purge
module load "Python/3.12.3-GCCcore-13.3.0"
module load "Java/17.0.6"
module load "OpenMPI/5.0.3-GCC-13.3.0"

# 2. Configurar Ambiente Python e Spark
source /projects/F202500010HPCVLABUMINHO/uminhocp043/env-spark/bin/activate
export PYTHONPATH=/projects/F202500010HPCVLABUMINHO/uminhocp043/env-spark/bin/python
export PYSPARK_PYTHON=/projects/F202500010HPCVLABUMINHO/uminhocp043/env-spark/bin/python
export PYSPARK_DRIVER_PYTHON=/projects/F202500010HPCVLABUMINHO/uminhocp043/env-spark/bin/python
export SPARK_HOME=/projects/F202500010HPCVLABUMINHO/uminhocp043/spark-3.5.4-bin-hadoop3
export PATH="${SPARK_HOME}/bin:${PATH}"

# 3. Configurar Workers (A magia do Cluster)
# Pega nos nomes dos nós alocados e diz ao Spark quem são os workers
scontrol show hostnames | sed -n '2,$p' > $SPARK_HOME/conf/workers
MASTER_NODE=$(scontrol show hostnames | head -n 1)

# 4. Iniciar o Cluster Spark (Master + Workers)
$SPARK_HOME/sbin/start-all.sh

# 5. Submeter o nosso script Python ao Cluster
# Aqui é que a Parte 3 acontece
spark-submit \
    --master spark://${MASTER_NODE}:7077 \
    --conf spark.executor.memory=20G \
    --conf spark.driver.memory=20G \
    baseline.py -m Jan

# 6. Parar o Cluster no fim
$SPARK_HOME/sbin/stop-all.sh