# mbaBigData
Ao tentar usar a primeira vez o pyspark (from pyspark import SparkContext) exibirá o seguinte erro:
"from pyspark import SparkContext
ModuleNotFoundError: No module named 'pyspark'"
Para resolver basta instalar o modulo do pyspark no pyp
pip2 install pyspark
pip install pyspark
O seguinte erro irá aparecer:
        File "<string>", line 1, in <module>
    ImportError: No module named setuptools
    
    ----------------------------------------
  Command "python setup.py egg_info" failed with error code 1 in /tmp/pip-build-v6hNb2/pyspark/

Para resolver rode o comando pip install --upgrade setuptools.

Logo depois execute 
pip2 install pyspark
