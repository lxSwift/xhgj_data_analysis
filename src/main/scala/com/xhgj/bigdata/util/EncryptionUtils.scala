package com.xhgj.bigdata.util
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.codec.binary.Base64
/**
 * @Author luoxin
 * @Date 2023/6/25 10:18
 * @PackageName:com.xhgj.bigdata.util
 * @ClassName: EncryptionUtils
 * @Description: 加密以及解密函数, 脱敏函数自定义
 * @Version 1.0
 */
object EncryptionUtils {
  // AES/ECB/PKCS5Padding 加密算法和填充方式
  private val cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
  private val key = new SecretKeySpec("GEl209zbw5$1Zd#429^F1008oKLqDfZ4".getBytes("UTF-8"), "AES") // 初始密钥

  /**
   * 加密函数
   *
   * @param inputStr 输入字符串
   * @return 加密后的字符串
   */
  def encrypt(inputStr: String): String = {
    // 加密操作
    cipher.init(Cipher.ENCRYPT_MODE, key)
    val encryptedBytes = cipher.doFinal(inputStr.getBytes("UTF-8"))
    val encryptedStr = Base64.encodeBase64String(encryptedBytes)
    encryptedStr
  }

  /**
   * 解密函数
   *
   * @param encryptedStr 加密后的字符串
   * @return 解密后的字符串
   */
  def decrypt(encryptedStr: String): String = {
    // 解码 Base64 编码
    val encryptedBytes = Base64.decodeBase64(encryptedStr)
    // 解密操作
    cipher.init(Cipher.DECRYPT_MODE, key)
    val decryptedBytes = cipher.doFinal(encryptedBytes)
    val decryptedStr = new String(decryptedBytes, "UTF-8")
    decryptedStr
  }

  /**
   * 脱敏函数
   *
   * @param inputStr 输入字符串
   * @return 脱敏后的字符串
   */
  def desensitize(inputStr: String): String = {
    val desensitizedStr = inputStr.replaceAll("\\d{4}(?=\\d{4})", "****") // 脱敏规则：替换连续四个数字
    desensitizedStr
  }

  def main(args: Array[String]): Unit = {
    val ao = "123.256"
    val c = encrypt(ao)
    println("encrypt="+c)
    println("decrypt="+decrypt(c))

  }
}
