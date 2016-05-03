
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.text.PDFTextStripper
import java.io._

object ExtractPDF {
  def getFiles(directory : String) : Array[java.io.File] = {
    try {
      val fileDirectory = (new File(directory)).listFiles()
      return fileDirectory
    } catch {
      case t: Throwable => {t.printStackTrace(); return new Array[java.io.File](0)}
    }  
  }
  
  def writepdf(file : java.io.File){
     try{
        val test = file.toString().split("/").toList.last.concat(".txt")
        println(test)
        val document=PDDocument.load(file)
        val stripper = new PDFTextStripper()
        val doc = stripper.getText(document)
        document.close()
        val bw = new BufferedWriter(new FileWriter(test))
        bw.write(doc)
        bw.close()
     } catch{
       case t: Throwable => None
     }
  }
  
  def extractPDF (files : Array[java.io.File]){
    try {
      for (file <- files) yield{
        writepdf(file)
      }
    }
    catch{
      case t: Throwable => None
    }
  }
  
  def main(args: Array[String]){
	for(arg<-args)
		print(arg)
    val files=getFiles(args(0))
    val content=extractPDF(files)
  }
}
