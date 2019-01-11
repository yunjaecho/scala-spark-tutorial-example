

def curry[A, B, C](f:(A, B) => C): A => (B => C) = {
  a => b => f(a, b)
}

def uncurry[A, B, C](f: A => B => C): (A, B) => C = {
  (a, b) => f(a)(b)
}

def compose[A, B, C](f: B => C, g: A => B): A => C = {
  a => f(g(a))
}


val f = (x: Double) => math.Pi / 2 - x
val cos = f andThen math.sin

println(f(2))
println(math.sin(f(2)))
println(cos(2))