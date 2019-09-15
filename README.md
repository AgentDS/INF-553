# INF-553

INF-553 2019 fall, by Prof. Anna Farzindar

[TOC]

## Basic Course Information



- Lec1: Introduction & MapReduce1







## Homework

__Environment:__ macOS Mojave 10.14.5

__Requirements:__ Python 3.6, Scala 2.11 and Spark 2.3.3

> Except the requirements above, you can only use standard python libraries



### Install

Before installing, make sure you have [Anaconda](https://www.anaconda.com/distribution/) as well as [Homebrew](https://brew.sh/).

#### Install Java JDK

Download Java JDK from the [link](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html), and open the ``dmg`` file to install it.

Then add the ``JAVA_HOME`` in ``~/.zshrc`` file (if using bash, just add this to ``~/.bash_profile`` file):

```bash
export JAVA_HOME="/Library/Java/JavaVirtualMachines/jdk1.8.0_221.jdk/Contents/Home"
```

save the changes and activate the change in the terminal:

```bash
source ~/.zshrc
```

or for bash:

```bash
source ~/.bash_profile
```

Some other installment tutorial suggests to set ``JAVA_HOME`` as ``usr/lib/jvm/xxx.jdk``, but this didn't work in my system.

Now check whether you have installed Java JDK successfully in terminl:

```bash
java -version
```

Mine is![javaversion](./Note/pic/javaversion.png)



> I used to try to install Java via ``brew``:
>
> ```bash
> brew cask install java
> ```
>
> which install Java 12. __However__, it seemed there's some problem with Java 12 and Spark 2.3.3, so I finally used Java 8 JDK ([this problem was mentioned here](https://towardsdatascience.com/how-to-get-started-with-pyspark-1adc142456ec)).



#### Install Spark

Open the [download link](https://archive.apache.org/dist/spark/spark-2.3.3/) and choose the version you want. Here I chose ``spark-2.3.3-bin-hadoop2.7.tgz``.

When download finished, unzip it and move it to your ``/opt`` folder:

```bash
tar -xzf spark-2.3.3-bin-hadoop2.7.tgz
mv spark-2.3.3-bin-hadoop2.7 /opt/spark-2.3.3
```

Create a symbolic link (assuming you have multiple spark versions):

```bash
sudo ln -s /opt/spark-2.3.3 /opt/spark
```

Then add Spark path in the ``~/.zshrc`` or ``~/.bash_profile`` file and activate it:

```bash
export SPARK_HOME="/opt/spark"
export PATH="$SPARK_HOME/bin:$PATH"
```

Now, run the example to see whether install successfully:

```bash
cd /opt/spark/bin
run-example SparkPi 10
```

My result:![sparkexample](./Note/pic/sparkexample.png)



#### Install Pyspark in conda environment

Create new conda environment:

```bash
conda create -n inf553 python=3.6
```

and activate the environment when finished

```bash
conda activate inf553
```

Now install ``Pyspark`` using ``conda``:

```bash
conda install -c conda-forge pyspark
```



Add these codes to ``~/.zshrc`` or ``~/.bash_profile`` file to enable python/ipython from conda environment to use PySpark:

```bash
export PYSPARK_PYTHON="/anaconda3/envs/inf553/bin/python"
export PYSPARK_DRIVER_PYTHON="/anaconda3/envs/inf553/bin/ipython"
```

Then activate the changes:

```bash
source ~/.zshrc
```

or

```bash
source ~/.bash_profile
```



#### Install Scala

Download, then run:

```bash
sudo cp -R scala-2.11.12.tgz /usr/local/scala
```

Add code to ``~/.zshrc`` or ``~/.bash_profile``:

```bash
export PATH="/usr/local/scala/bin:$PATH"
```





### Homework Details

- HW1

