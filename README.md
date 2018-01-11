# Publishing-time-based Topic Derivation

`Author:Sphinx Chen`
* This work includes some implementations of topic derivation model, such as intJNMF, tNMijf and PTD.
* The implementation of the PTD is completed with the thesis of my M.S. degree in computer science.
* The twitter dataset reference: 
[`Twitter Sentiment Corpus`](http://www.sananalytics.com/lab/twitter-sentiment/)
[`Sentiment140`](http://help.sentiment140.com/for-students/)

### Table Content
* Getting Started
* Parameter setting
* Dataset format
* Usage
* Measure Evaluation
* Performance compare

## Tutorial

### Getting Started
1. Environment check
JDK 1.8+
Spark 2.0.2+
2. Download jar directly or build it by Maven
>First--Package the extra marlin`s library
>>mvn install:install-file -Dfile=/usr/lib/marlin-0.4-SNAPSHOT.jar -DgroupId=edu.nju.pasalab >>-DartifactId=marlin -Dversion=0.4-SNAPSHOT -Dpackaging=maven-archetype 
>>-DlocalRepositoryPath=<home_directory>/.m2

>Second--build it
>>mvn clean install 

3. Decide the parameter(iter, factor, top, etc...)
4. Prepare the Dataset for the fixed format or you can prepare it by yourself, beware of the format as the sample file.

### Parameter description
The parameter is not ordered, so you can decide the order by yourself.

`-iters`: The number of iterations for the topic models. The default value is 10.
`-factor`: The number of result topics. The default value is 5.
`-top`: The number of the top-N result for each topic. The default value is 10.
`-input`: The path of the twitter dataset.
`-output`: The path of the result. 
`-model`: Determine the topic deriving model, such as PTD, intJNMF, tNMijF, vector and coherence.
`-cohInput`: The path of topic word file.
`-cores`: The number of cores which you set in the environment. The default value is 2.
`-threshold`: The threshold of broadcasting variable. The unit of this parameter is MB, default value is 3000.
`-sample`: The number of random sampling initial dataset. The default value is 0(all).
`-timeParam`: The time decay rate used in tNMijF model.

### Dataset format
Just use following three field: 
Column Number|Value|Example
:--:|--|:--
2 | the date of the tweet |Sat May 16 23:58:44 UTC 2017
4 | the user that tweeted |Peter
5 | the text of the tweet |Peter is cool

### Usage

#### PTD / intJNMF
**Example:**
`/usr/lib/spark/bin/spark-submit <user_defined_path>/app.jar -iters 10 -factor 5 -top 10 -input <input_file_path> -output <output_folder_path> -model < PTD or intJNMF > [-cores 10] [-sample 100] [-threshold 3000]`
where hyper-parameters in `[ ]` are optional.

**Output:**
System console shows the current `max topic coherence value`, `average topic coherence value` and then output a folder `result_<create_date>`, there are `parameter`, `part-00000`(the top-word list) inside the `result` folder.

#### tNMijF
**Example:**
*you must decide the timeParam first, please refer to* **[2]**.
`/usr/lib/spark/bin/spark-submit <user_defined_path>/app.jar -iters 10 -factor 5 -top 10 -input <input_file_path> -output <output_folder_path> -model tNMijF -timeParam 100 [-cores 8] [-sample 100]`

**Output:**
System console shows the current `max topic coherence value`, `average topic coherence value` and then output a folder `result_<create_date>`, there are `parameter`, `part-00000`(the top-word list) inside the `result` folder.

### Measure Evaluation

#### topic coherence
**Example:**
*use previous topic word list(part-00000) to be <coherence_input_path>*
`/usr/lib/spark/bin/spark-submit <user_defined_path>/app.jar -input <input_file_path> -output <output_folder_path> -model coherence -cohInput <coherence_input_path>`

**Output:**
System console shows the current `max topic coherence value` and `average topic coherence value`.

#### vector 
**Example**
`/usr/lib/spark/bin/spark-submit <user_defined_path>/app.jar -iters 10 -factor 5 -top 10 -input <input_file_path> -output <output_folder_path> -model vector`

**Output**
The two folders `wordVector_<create_date>` and `corpus_<create_date>` are produced, show the latent vector for each word and the word corpus.


## Issues

*If any problems, please give me a feedback, thanks*
## Contact

[`Linkedin`](http://www.linkedin.com/in/sphinx-chen)
[`Email`](mailto:hot.jun@msa.hinet.net)
[`CakeResume`](https://goo.gl/Pa9469)
## Note

*The importation of [`Marlin's library`](https://goo.gl/VfWG5y) must be tuned carefully*
*Special thanks to Professor KWang and everyone who helped me finish these experiments*
## Reference

intJNMF:
[1]	R. Nugroho, J. Yang, Y. Zhong, C. Paris and S. Nepal, “Deriving Topics in Twitter by Exploiting Tweet Interactions,” in Proc. IEEE International Congress on Big Data, 2015.

tNMijF:
[2]	R. Nugroho, W. Zhao, J. Yang, C. Paris, and S. Nepal, “Using time-sensitive interactions to improve topic derivation in twitter,” World Wide Web, vol. 20, no. 1, pp. 61–87, Jun. 2016.

:smile: