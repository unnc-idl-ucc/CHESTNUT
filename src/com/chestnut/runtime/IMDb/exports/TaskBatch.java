package com.chestnut.runtime.IMDb.exports;

import com.chestnut.runtime.Mahout.MahoutRecommender;
import com.chestnut.runtime.dal.ma.DataSession;
import com.deeFnFProcessor.fileCounter.FilesCounter;
import com.deeFnFProcessor.fileCounter.FnFOperator;
import com.deeFnFProcessor.fileCounter.FnFStore;

public class TaskBatch {

    private FnFStore _batchDirs;
    
    // data/ProductEnv/IMDb/Exports
    public TaskBatch(String dir) {
        InitBatchDirs(dir);
    }
    
    /***
     * 根据�� IMDb 导出文件构建�
     * 1、补全信息后导入网站的用户评分电影信息文�
     * 2、补全信息后导入系统的用户评分电影信息文�
     * @return
     */
    public String[] RunBatch() {
        if(_batchDirs != null) {
            String[] dirsList = _batchDirs.getFilesAbsPath();
            String[] sysResultFilesPath = new String[dirsList.length];
            for(int i=0; i<dirsList.length; i++) {
                System.out.println("[TaskBatch.RunBatch]: path, " + dirsList[i]);
                sysResultFilesPath[i] = BuildSingleExportsResults(dirsList[i]);
                
                // TODO
                MahoutRecommender IBUBHandler = new MahoutRecommender("data/ProductEnv/newCombines/ratings.csv");
                IBUBHandler.Recommend(5, "200000", sysResultFilesPath[i]);
            }
            return sysResultFilesPath;
        }
        return null;
    }
    
    /***
     * 初始化目录，清空目录原有结果
     * @param dir
     */
    private void InitBatchDirs(String dir) {
        FilesCounter fc = new FilesCounter(dir + "/Exports");
        FilesCounter fc_toSys = new FilesCounter(dir + "/results/toSys");
        FilesCounter fc_toWeb = new FilesCounter(dir + "/results/toWeb");
        System.out.println("[TaskBatch.InitBatchDirs]: file counts = " + fc.getFileCount() + ", folder counts = " + fc.getFolderCount());
        
        // 查看导入补全文件的文件夹中是否有文件，如果有则将旧文件移入历史文件夹中�
        if(fc_toSys.getFileCount() > 0) {
            FnFOperator fileOperator = new FnFOperator(dir + "/results/toSys");
            fileOperator.MoveFileBatch(dir + "/results/toSys/history");
        }
        
        if(fc_toWeb.getFileCount() > 0) {
            FnFOperator fileOperator = new FnFOperator(dir + "/results/toWeb");
            fileOperator.MoveFileBatch(dir + "/results/toWeb/history");
        }
        
        _batchDirs = new FnFStore(fc.getFileCount(), fc.getFolderCount(), dir + "/Exports");
    }
    
    private String BuildSingleExportsResults(String singlePath) {

        String fileName = _batchDirs.GetFileName(singlePath);
        DataSession exportFileDataSession = ExportFileParser.GetExportsDataSession(singlePath, null, "Const", "export_" + fileName);
        
        System.out.println("[TaskBatch.BuildSingleExportsResults]: export DataSession." + exportFileDataSession.sessionName + " size = " + exportFileDataSession.dataRecordSize);
        
        ImportDataFactory importHander = new ImportDataFactory();
        importHander.SetExportFileDataSession(exportFileDataSession);
        importHander.BuildSysUsageDataSession().ExportToESV("data/ProductEnv/IMDb/results/toSys", "export_sys_" + fileName, ",");
        String fileFullPath = "data/ProductEnv/IMDb/results/toSys/export_sys_" + fileName + ".csv";
        
        // Expand data file to be used to import user profile to the user`s information checking page.
        importHander.BuildWebUsageDataSession().ExportToESV("data/ProductEnv/IMDb/results/toWeb", "export_web_" + fileName, ",");
        
        return fileFullPath;
    }
    
}
