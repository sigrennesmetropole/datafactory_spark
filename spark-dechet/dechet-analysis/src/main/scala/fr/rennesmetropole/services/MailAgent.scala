package fr.rennesmetropole.services

import java.util.Properties
import com.typesafe.config.{Config, ConfigFactory}
import fr.rennesmetropole.tools.Utils
import fr.rennesmetropole.tools.Utils.logger
import javax.mail._
import javax.mail.internet._
import fr.rennesmetropole.tools.Utils.log
object MailAgent {

  private val config: Config = ConfigFactory.load()
  private val to : String = Utils.tableVar("email","to")
  private val from : String = Utils.tableVar("email","from")
  private val subject : String = Utils.tableVar("email","subject")
  private val smtpHost : String = Utils.tableVar("email","smtpHost")
  private val port : String = Utils.tableVar("email","port")
  private val user : String = Utils.tableVar("email","user")
  private val password : String = Utils.tableVar("email","password")

  def setProperties: Properties ={
    logger.info("setProperties")

    logger.info("Set up properties with : " +
      "\nmail.smtp.host: "+ smtpHost+
      "\nmail.smtp.user: "+ user +
      "\nmail.smtp.password: "+ password +
      "\nmail.smtp.port: "+ port +
      "\n\"mail.smtp.auth: true"+
      "\nmail.smtp.starttls.enable: tre")

    val properties = System.getProperties
    properties.put("mail.smtp.host", smtpHost)
    properties.put("mail.smtp.user", user)
    properties.put("mail.smtp.password", password)
    properties.put("mail.smtp.auth", "true")
    properties.put("mail.smtp.port", port)
    properties.put("mail.smtp.starttls.enable", "false")

    properties
  }

  def getMailReady(subject:String): Message ={
    logger.info("getMailReady")
    val auth:Authenticator = new Authenticator() {
      override def getPasswordAuthentication = new
          PasswordAuthentication(user, password)
    }

    val properties : Properties = setProperties
    val session = Session.getInstance(properties,auth)
    val message = new MimeMessage(session)

    message.setFrom(new InternetAddress(from))
    val dest = InternetAddress.parse(to).asInstanceOf[Array[Address]]  // to peut être une liste d'adresse email (ex : "abc@abc.example,abc@def.com,ghi@abc.example" )
    log("destinataire:")
    log(dest.mkString(";"))
    message.setRecipients(Message.RecipientType.TO, dest)
    message.setSubject(subject)

    message
  }

  def sendMail(content : String, subject : String): Unit ={
    val execute : Boolean =  Utils.tableVar("email","execute").toBoolean
    logger.warn("SendMail should be executed ? " + execute)
    if(execute){
      logger.warn("SendMail content is :" + content)
      if(content.length != 0){
        try{
          val message : Message =  getMailReady(subject)
          message.setText(content)
          Transport.send(message)
        }
        catch{
          case e : Throwable => logger.error("Something went wrong while setting up / sending mail! : " + e.toString)
        }
      }
    }
  }

  def mail_template(jobs:String,environnement:String, body:String, DATE:String): String ={
    var mail = s"""   /!\\   ALERTE SPARK    /!\\
    Jobs: ${jobs}
    Date correspondant à l'éxecution: ${DATE}
    Environnement: ${environnement}
    Message : ${body}
    """

    mail
  }

}
