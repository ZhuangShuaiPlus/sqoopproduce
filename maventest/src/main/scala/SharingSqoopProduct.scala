import java.sql.{ResultSet, Statement}

import bean.{ShardingDT, ShardingTableInfo}
import com.tpln.{DataBaseInfo, FieldInfo}
import com.tpln.util.SqlConnManagement

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @author Shuai
 * @create 2021-01-15 14:54
 */
object SharingSqoopProduct {
  def main(args: Array[String]): Unit = {
    println("a")

    //需要的类A=>{
    //库 表 list<filedInfo>,
    //
    // }
    //1.从hive取出表结构和库表名，并去掉前缀
    // 返回类型：list<{库，表, list<{字段类}>}>

    //测试url
    val hiveMetaUrl = "jdbc:mysql://172.28.30.95:3306/hive"
    val hiveMetaUser = "root"
    val hiveMetaPasswd = "root"
    val hiveMetaManagement: SqlConnManagement = new SqlConnManagement(hiveMetaUrl, hiveMetaUser, hiveMetaPasswd);
    val hiveMetaStatement: Statement = hiveMetaManagement.conn()
    val hiveMetaSql =
      """
        |SELECT * FROM
        |(
        |	SELECT  d.`NAME` ,t.`TBL_ID`, t.`TBL_NAME` , t2.`COLUMN_NAME` ,t2.`integer_idx`
        |	FROM `DBS` d
        |	JOIN `TBLS` t
        |	ON  d.`DB_ID` = t.`DB_ID`
        |	JOIN `SDS` s
        |	ON t.SD_ID = s.SD_ID
        |	JOIN `COLUMNS_V2` t2
        |	ON s.`CD_ID` = t2.`CD_ID`
        |	WHERE d.`NAME` LIKE 'ods%' and t.`TBL_NAME` LIKE 'ods%'
        |)t3
        |ORDER BY t3.`NAME`,t3.`TBL_NAME`,t3.`integer_idx`
        |""".stripMargin
    val hiveRS: ResultSet = hiveMetaStatement.executeQuery(hiveMetaSql)
    val tmpTableInfo: ListBuffer[(String, String, String)] = ListBuffer[(String, String, String)]()
    while (hiveRS.next()) {
      val dbname: String = hiveRS.getString("NAME")
      val tblname: String = hiveRS.getString("TBL_NAME")
      val filedName: String = hiveRS.getString("COLUMN_NAME")
      tmpTableInfo.append((dbname, tblname, filedName))
    }
    val tmpTableInfoField: ListBuffer[(String, String, String)] = tmpTableInfo.map(t => {
      var db: String = ""
      var tbl: String = ""
      //      try {
      db = t._1.substring(4)
      tbl = t._2.substring(4)
      //      }
      //      catch {
      //        case ex: Exception => {
      // 对异常处理
      //          println("发生了异常1" + t)
      //        }
      //      }
      (db, tbl, t._3)
    })
    val tupleToTuples: Map[(String, String), ListBuffer[(String, String, String)]] = tmpTableInfoField.groupBy(t => {
      (t._1, t._2)
    })
    var shardingTableInfos = new ListBuffer[ShardingTableInfo]()
    tupleToTuples.map(kv => {
      val k: (String, String) = kv._1
      val v: ListBuffer[(String, String, String)] = kv._2
      val filedList: ListBuffer[FieldInfo] = v.map(tp => {
        new FieldInfo(tp._3)
      })
      shardingTableInfos.append(new ShardingTableInfo(k._1, k._2, filedList))
    })

    //2.手动列出得到，drds链接和库的映射
    // 返回类型：map<库，drds链接类>
    val drdsMap = new mutable.LinkedHashMap[String, DataBaseInfo]()
    drdsMap.put("english_read", new DataBaseInfo("jdbc:mysql://drdsbggado8i32w8.drds.aliyuncs.com:3306/", "tidb_ro", "274AHguQQfgUS98gVseu"))
    drdsMap.put("english_oral", new DataBaseInfo("jdbc:mysql://drdsbgga1s358k06.drds.aliyuncs.com:3306/", "tidb_ro", "274AHguQQfgUS98gVseu"))
    drdsMap.put("english_rank", new DataBaseInfo("jdbc:mysql://drdsbggasv1o4o86.drds.aliyuncs.com:3306/", "tidb_ro", "274AHguQQfgUS98gVseu"))
    drdsMap.put("english_ebook", new DataBaseInfo("jdbc:mysql://drdsbggasv1o4o86.drds.aliyuncs.com:3306/", "tidb_ro", "274AHguQQfgUS98gVseu"))
    drdsMap.put("english_gold", new DataBaseInfo("jdbc:mysql://drdsbggasv1o4o86.drds.aliyuncs.com:3306/", "tidb_ro", "274AHguQQfgUS98gVseu"))
    drdsMap.put("english_game", new DataBaseInfo("jdbc:mysql://drdsbgga17222szn.drds.aliyuncs.com:3306/", "tidb_ro", "274AHguQQfgUS98gVseu"))
    drdsMap.put("english_etp", new DataBaseInfo("jdbc:mysql://drdsbgga17222szn.drds.aliyuncs.com:3306/", "tidb_ro", "274AHguQQfgUS98gVseu"))
    drdsMap.put("english_basedata", new DataBaseInfo("jdbc:mysql://drdsbggasxc095l0.drds.aliyuncs.com:3306/", "tidb_ro", "274AHguQQfgUS98gVseu"))
    drdsMap.put("english_agent", new DataBaseInfo("jdbc:mysql://drdsbggasxc095l0.drds.aliyuncs.com:3306/", "tidb_ro", "274AHguQQfgUS98gVseu"))
    drdsMap.put("english_book", new DataBaseInfo("jdbc:mysql://drdsbggasxc095l0.drds.aliyuncs.com:3306/", "tidb_ro", "274AHguQQfgUS98gVseu"))
    drdsMap.put("english_parent", new DataBaseInfo("jdbc:mysql://drdsbggasxc095l0.drds.aliyuncs.com:3306/", "tidb_ro", "274AHguQQfgUS98gVseu"))
    drdsMap.put("english_skill", new DataBaseInfo("jdbc:mysql://drdsbggasxc095l0.drds.aliyuncs.com:3306/", "tidb_ro", "274AHguQQfgUS98gVseu"))
    drdsMap.put("english_student", new DataBaseInfo("jdbc:mysql://drdsbgga581301b3.drds.aliyuncs.com:3306/", "tidb_ro", "274AHguQQfgUS98gVseu"))
    drdsMap.put("english_teacher", new DataBaseInfo("jdbc:mysql://drdsbgga581301b3.drds.aliyuncs.com:3306/", "tidb_ro", "274AHguQQfgUS98gVseu"))
    drdsMap.put("english_video", new DataBaseInfo("jdbc:mysql://drdsbgga581301b3.drds.aliyuncs.com:3306/", "tidb_ro", "274AHguQQfgUS98gVseu"))


    //3.得到库表对应的drds链接。
    // 返回类型：list<{{库，表, list<{字段类}>},drds链接类}> =>(group by key(链接)) list<{drds链接，list<{库，表, list<{字段类}>}>}>
    for (tblinfo <- shardingTableInfos) {
      tblinfo.drdsLink_=(drdsMap.getOrElse(tblinfo.databaseName, new DataBaseInfo()))
    }
    val drdsShardingTable: Map[DataBaseInfo, ListBuffer[ShardingTableInfo]] = shardingTableInfos.groupBy(s => {
      s.drdsLink
    })


    //4.对每个库表 show topology from 库表
    // 返回类型：list<{库，表，list<{字段类}>，list<{分库，分表}>}>
    drdsShardingTable.map(elem => {
      val drdslink: DataBaseInfo = elem._1
      val databaseinfo: ListBuffer[ShardingTableInfo] = elem._2
      //      for (elem <- databaseinfo) {
      //        val management = new SqlConnManagement(drdslink.getUrl(),drdslink.getUser,drdslink.getPassword)
      //      }
      val management = new SqlConnManagement(drdslink.getUrl(), drdslink.getUser, drdslink.getPassword)
      val statement: Statement = management.conn()
      databaseinfo.map(d => {
        val rs: ResultSet = statement.executeQuery("show topology from " + d.databaseName + "." + d.tableName)
        val listShardingDT = new ListBuffer[ShardingDT]()
        while (rs.next()) {
          val sdbname: String = hiveRS.getString("GROUP_NAME")
          val stblname: String = hiveRS.getString("TABLE_NAME")
          listShardingDT.append(ShardingDT(sdbname, stblname))
        }
        d.shardingDBTbl_=(listShardingDT)
      })
    })

    //5.从每个rds取出分库和分表
    // 返回类型：list<{rds链接类,分库，分表}> => map<{分库，分表}，rds链接类>
    new DataBaseInfo()


    //6.对从4得到的结构使用5添加链接值
    // 返回类型：list<{库，表，list<{字段类}>，list<{分库，分表，rds链接类}>}> =>(group by key(分库名)) list<{库，表，list<{分库，list<{分表，rds链接类}>}>}>


  }

}
