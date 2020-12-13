package tech.dnaco.storage.net;

import java.io.File;
import java.util.Map;

import com.gullivernet.commons.database.DbTableModel;
import com.gullivernet.commons.database.connection.DbInfo;
import com.gullivernet.commons.database.tools.ModelGenerator;
import com.gullivernet.commons.io.IOUtil;

import tech.dnaco.logging.Logger;

public class Model {
  public static void main(final String[] args) throws Exception {
    final String dbUrl = "jdbc:mysql://127.0.0.1:3310/goal_dev?user=mdcdev01user&password=I3aKcbxiam&autoReconnect=true";

    final File modelDir = new File("dnaco-storage-server/src/main/java/tech/dnaco/storage/net/dumbdb/");
    final File daoDir   = new File("dnaco-storage-server/src/main/java/tech/dnaco/storage/net/dumbdb/");
    final File testDir  = new File("dnaco-storage-server/src/main/java/tech/dnaco/storage/net/dumbdb/");
    modelDir.mkdirs();
    daoDir.mkdirs();
    testDir.mkdirs();

    final long startTime = System.currentTimeMillis();
    final ModelGenerator mg = new ModelGenerator("tech.dnaco.storage.net.dumbdb");
    final Map<String, DbTableModel> model = DbTableModel.loadTableModels(DbInfo.fromUrl(dbUrl));
    for(final DbTableModel table : model.values()) {
      if (!table.getTableName().startsWith("goal_d")) continue;

      final String modelFileName = String.format("%sRecord.java", mg.getHumanName(table.getTableName()));
      Logger.info("generate {} model: {}", table.getTableName(), modelFileName);
      final File modelFile = new File(modelDir, modelFileName);
      IOUtil.writeFile(modelFile, mg.generateJavaModel(table));

      final String daoFileName = "Dao" + modelFileName;
      Logger.info("generate {} dao: {}", table.getTableName(), daoFileName);
      final File daoFile = new File(daoDir, daoFileName);
      IOUtil.writeFile(daoFile, mg.generateJavaDao(table));
    }
  }

}
