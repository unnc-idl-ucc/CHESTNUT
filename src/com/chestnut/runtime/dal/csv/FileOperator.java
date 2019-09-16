package com.chestnut.runtime.dal.csv;

import java.io.File;

public class FileOperator {
	
	public FileOperator(){
		
	}
	
	public boolean CreateDirectory(String destDirName){
		File dir = new File(destDirName);
		if(dir.exists()){
			System.out.println("[Warning] Directory " + destDirName + " created failed, directory existed!");
			return false;
		}
		if(!destDirName.endsWith(File.separator)){
			destDirName = destDirName + File.separator;
		}
		if(dir.mkdirs()){
			System.out.println("[Noted] Directory " + destDirName + " created successfully!");
			return true;
		}else{
			System.out.println("[Noted] Directory " + destDirName + " created failed!");
			return false;
		}
	}
	
	public void DeleteFolder(String folderDir){
		DeleteAllFiles(folderDir);
		File folderPath = new File(folderDir);
		folderPath.delete();
	}
	
	public boolean DeleteAllFiles(String rootDir){
		boolean flag = false;
		File file = new File(rootDir);
		if(!file.exists()) return flag;
		if(!file.isDirectory()) return flag;
		
		String[] tempList = file.list();
		File temp = null;
		for(int i=0; i<tempList.length; i++){
			if(rootDir.endsWith(File.separator)){
				temp = new File(rootDir + tempList[i]);
			}else{
				temp = new File(rootDir + File.separator + tempList[i]);
			}
			
			if(temp.isFile()) temp.delete();
			
			if(temp.isDirectory()){
				DeleteAllFiles(rootDir + File.separator + tempList[i]);
				DeleteFolder(rootDir + File.separator + tempList[i]);
			}
			flag = true;
		}
		return flag;
	}
}
