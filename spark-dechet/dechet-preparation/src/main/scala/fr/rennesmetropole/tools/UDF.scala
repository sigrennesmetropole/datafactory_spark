package fr.rennesmetropole.tools

object UDF {

    def udfToString(valeur:String): String ={
        valeur
    }

    def udfToInt(valeur:String): Int ={
        try {
          if(valeur.contains(".")) {
            valeur.toDouble.toInt
          }else {
            valeur.toInt
          }
        }catch{
             case e : Throwable => 
             println("ERREUR, essaie de transformer "+valeur+" en Int \n" +e)
             null.asInstanceOf[Int]
        }
    }

    def udfToDouble(valeur:String): Double ={
        var current = valeur
        try {
            if(current.contains(',')){
                current = current.replace(',','.')
                BigDecimal(current).setScale(15,BigDecimal.RoundingMode.DOWN).toDouble
            }else {
                
                BigDecimal(current).setScale(15,BigDecimal.RoundingMode.DOWN).toDouble
            }
            
        }catch{
             case e : Throwable => 
             //println("ERREUR, essaie de transformer "+valeur+" en Double \n" +e)
             null.asInstanceOf[Double]
        }
    }

    def udfToBoolean(valeur:String): Boolean ={
        try {
            valeur.toBoolean
        }catch{
             case e : Throwable => 
             //println("ERREUR, essaie de transformer "+valeur+" en Boolean \n" +e)
             null.asInstanceOf[Boolean]
        }
    }

    def udfToFloat(valeur:String): Float ={
        try {
            valeur.toFloat
        }catch{
             case e : Throwable => 
             //println("ERREUR, essaie de transformer "+valeur+" en Boolean \n" +e)
             null.asInstanceOf[Float]
        }
    }

}