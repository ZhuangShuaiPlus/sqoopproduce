package bean

import java.util
import java.util.LinkedList

import com.tpln.{DataBaseInfo, FieldInfo}

import scala.collection.mutable.ListBuffer

/**
 * @author Shuai
 * @create 2021-01-15 15:12
 */
class ShardingTableInfo() {



  var databaseName: String = _
  var tableName: String = _
  var tableFields: ListBuffer[FieldInfo] = _
  var drdsLink: DataBaseInfo = _
  var shardingDBTbl:ListBuffer[ShardingDT] = _


  def this (databaseName: String, tableName: String) { // 辅助构造函数，使用this关键字声明
    this () // 辅助构造函数应该直接或间接调用主构造函数
    this.databaseName = databaseName
    this.tableName = tableName
  }

  def this(databaseName: String, tableName: String, tableFields: ListBuffer[FieldInfo]) { // 辅助构造函数，使用this关键字声明
    this(databaseName, tableName)
    this.tableFields = tableFields;
  }


}

case class ShardingDT(db:String ,table:String){}