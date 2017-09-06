def sum(a: Int,b: Int)=a+b
val s = sum(20, 21)

def sqrt(a: Double) = {
  var y: Double = 1
  while (if (a-y*y > 0) a-y*y > a*0.0000000000001 else y*y-a > a*0.0000000000001){
    y=(a/y+y)/2
  }
  y
}

val sq1 = sqrt(2)
val sq2 = sqrt(1e-6)
val sq3 = sqrt(1e60)

def pascal(column: Int, row: Int): Int = {
  if (column == 0 || column == row) 1
  else
    pascal(column,row-1)+pascal(column-1,row-1)
}

val pa1 = pascal(4,8)

def balance(chars: List[Char]): Boolean = {
  def f(chars: List[Char], numOpens: Int): Boolean = {
    if (chars.isEmpty) {
      numOpens == 0
    } else {
      val h = chars.head
      val n =
        if (h == '(') numOpens + 1
        else if (h == ')') numOpens - 1
        else numOpens
      if (n >= 0) f(chars.tail, n)
      else false
    }
  }
  f(chars, 0)
}

val b1 = balance(")(".toList)

var a = Array(1,2,3,4,5)

a.map(Math.pow(_,2).toInt).reduceLeft(_+_)


"sheena is a punk rocker she is a punk punk".split(" ").map(s => (s, 1)).groupBy(p => p._1).mapValues(v => v.length)

/* first : split among spaces
  second : create array of tuple for each word with (word, 1)
  third : groupe by tuple and increment value for each occurence
  fourth : print number of occurences of each word
 */

"sheena is a punk rocker she is a punk punk".split(" ").map((_, 1)).groupBy(_._1).mapValues(v => v.map(_._2).reduce(_+_))

/* first : split among spaces
  second : create array of tuple for each word with (word, 1)
  third : groupe by tuple and increment value for each occurence
  fourth : print number of occurences of each word

  it's the same with a different implementation
 */