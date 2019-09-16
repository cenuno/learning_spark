// Section 1: Bringing data from the cluster to the client

// Load data
val rawblocks = sc.textFile("raw_data/linkage.csv")

// View first line (i.e. the header of the csv)
rawblocks.first

// Use first method for one line; use collect method to return all lines
// Strike a balance using take method to return back the first X number of lines
val head = rawblocks.take(10)

// Confirm only 10 lines were returned
head.length

// To improve reading experience in the REPL, use the foreach method
// in conjunction with println to print each array on its own line
head.foreach(println)

/* Note: above is an example of a common functional programming pattern,
         where we pass one function (println) as an argument to another
         function (foreach) in order to perform some action.

         This is similar to R's lapply or apply methods that process vectors
         in order to avoid for loops.
*/

// we'll want to filter out the header from future analysis
// Note: unlike Python & R, Scala requires that types be statically typed
def isHeader(line: String): Boolean = {
  line.contains("id_1")
}

// inspect function
head.filter(isHeader).foreach(println)

// let's get all rows of data except the header rows
// option 1: use of filterNot() method
head.filterNot(isHeader).length

// option 2A: use Scala's support for anonymous functions to negete the isHeader
//           function from inside filter:
head.filter(x => !isHeader(x)).length

// option 2B: Scala programmers hate typing, so Scala has lots of features
//            designed to reduce the amoung of typing they have to do
head.filter(!isHeader(_)).length

// Section 2:  Shipping Code from the Client to the Cluster
// goal: apply code to millions of linkage records contained in our cluster
//       represented by the rawblocks RDD in Spark
val noheader = rawblocks.filter(x => !isHeader(x))

// Syntax is exaclty the same as when we filtered the smaller head object!!!
/* Note: this is incredibly powerful.

         It means we can interactively develop and debug our data-munging code
         against a small amount of data that we sample from the cluster,
         and then ship that code to the cluster to apply it to the entire data
         set when we're ready to transform the entire data set.

         Best of all, we never have to lave the shell. There really isn't
         another tool that gives you this kind of experience.

         In the next several sections, we'll use this mix of local development
         and testing and cluster computation to perform more munging and
         analysis of the record linkage data, but if you need a moment to
         drink in the new world of awesome that you have just entered,
         we certainly understand.
*/

// Section 3: Structuring Data with Tuples and Case Classes
/* goal: parse each line into a tuple with five values
         - a String of the filename
         - an Integer ID of the first patient
         - an Integer ID of the second patient
         - an Array of 9-doubles representing the match score
         - a Boolean (with NaN values for missing values)
           indictating whether or not the fields matched


         Unlinke Python, Scala does not have a built-in method for parsing
         comma-separated strings, so we'll need to do a bit of the legwork
         ourselves.
*/
val line = head(5)
val pieces = line.split(",")

/* Note: in Scala, accessing array elements is a function call, not a
         special operator. That's why we accessed the elements of the head
         array using paranetheses instead of brackets.

         Scala allows classes to define a special function named apply that
         is called when we treat an object as if it were a function, so
         head(5) is the same thing as head.apply(5)
*/

// convert the individual elements of pieces to the appropriate type
val group = pieces(0).toString
val id1 = pieces(1).toInt
val id2 = pieces(2).toInt
val matched = pieces(12).toBoolean

// convert the double-valued score fields - all nine of them
val rawscores = pieces.slice(3, 12)
rawscores.map(s => s.toDouble())  // error! Can't convert '?' to double

def toDouble(s: String) = {
  if ("?".equals(s)) Double.NaN else s.toDouble
}

val scores = rawscores.map(toDouble)

// bring it all together into one function
def parse(line: String) = {
  val pieces = line.split(",")
  val group = pieces(0).toString
  val id1 = pieces(1).toInt
  val id2 = pieces(2).toInt
  val scores = pieces.slice(3, 12).map(toDouble)
  val matched = pieces(12).toBoolean
  (group, id1, id2, scores, matched)
}

val tup = parse(line)

// retrieve the values of individual fields from our tuple by using the
// positional functions, starting from _1 or
// starting from 0 via the productElement() method
// note: you can also get the size via productArity() method
tup._1
tup.productElement(0)
tup.productArity

/* Note: referencing items by position isn't good coding. What we would like
         to do is refernece elements by a meaningful name.

         Fortunately, Scala provides a convenient synteax for creating these
         records, called case classes. A case class is a simple type of
         immuatable class that comes with implementations of all of the basic
         Java class methods, like toSTring, equals, and hashCode, which
         makes them very easy to use.

         Let's declare a case class for our record linkage data
*/
case class MatchData(group: String, id1: Int, id2: Int,
                     scores: Array[Double], matched: Boolean)

// update our parse method to return an instance of our MatchData case class
// note: this overwrites our previous parse() method to return case class
//       instead of a tuple
def parse(line: String) = {
  val pieces = line.split(",")
  val group = pieces(0).toString
  val id1 = pieces(1).toInt
  val id2 = pieces(2).toInt
  val scores = pieces.slice(3, 12).map(toDouble)
  val matched = pieces(12).toBoolean
  MatchData(group, id1, id2, scores, matched)
}

val md = parse(line)

// now we can reference our elements by name rather than by position
md.group
md.id1
md.scores

// apply parse() to all elements in the head array, except for the header line
val mds = head.filter(x => !isHeader(x)).map(x => parse(x))

// apply parse() to the all the data stored in noheader
val parsed = noheader.map(line => parse(line))

/* Note: unlike the mds array that we generated locally, the parse function has
         not actually been applied to the data on the cluster yet. Once we
         make a call to the parsed RDD that requires some output, the parse
         function will be applied to convert each String in the noheader RDD
         into an instance of our MatchData class. If we make another call to
         the parsed RDD that generates a different output, the parse function
         will be applied to the input data again.

         This isn't an optimal use of our cluster resources; after the data has
         been parsed once, we'd like to save the data in its parsed form on the
         cluster so that we don't have to reparse it every time we want to
         ask a new question of the data.

         Spark supports this use case by allowing us to signal that a given
         RDD should be cached in memory after it is generated by calling the
         cache method on the instance.

         rdd.cache() is shorthand for rdd.persist(StorageLevel.MEMORY),
         which stores the RDD as unserialized Java objects.

         Deciding when to cache data can be an art. The decision typically
         involves trade-offs between space and speed, with the specter of
         garbage collecting looming overhead to occasionally confound things
         further. In general, RDDs should be cached when they are likely
         to be referenced by multiple actions and are expensive to
         regenerate.
*/
parsed.cache()

// Section 4: Aggregations

// calculate the number of records that are matches versus the number of
// records that are not.
val grouped = mds.groupBy(md => md.matched)

// get the counts by calling the mapValues method of grouped, which is like
// map method except that it only operates on the values in the Map object
// and gets the size of each array
grouped.mapValues(x => x.size).foreach(println)

/* Note: When we are performing aggregations on the data in the cluster, we
         always have to be mindful of the fact that the data we are analyzing
         is stored across multiple machines, and so our aggregations will
         require moving data over the network that connects the machines.

         Moving data across the network requires a lot of computational
         resources: including determining which machines each record will
         be transferred to, serializing the data, compressing it,
         sending it over the wire, decompressing and then deserializing the
         results and finally perfomring computations on the aggregated data.

         To do this quickly, it is important that we try to minimize the amount
         of data that we move around; the more filtering that we can do to the
         data before performing aggregations, the faster we will get an
         answer to our question.
*/

// Section 5: Creating Histograms


