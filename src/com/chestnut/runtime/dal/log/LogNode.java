package com.chestnut.runtime.dal.log;

import java.util.ArrayList;
import java.util.List;

import com.chestnut.runtime.dal.ma.DataSession;

public class LogNode {
    
    private List<LogNode> _children;
    private DataSession _nodeData;
    private LogNode _parentNode = null;
    
    public LogNode(DataSession nodeData){
        _nodeData = nodeData;
        _children = new ArrayList<LogNode>();
    }
    
    public void SetParent(LogNode parentNode){
        _parentNode = parentNode;
    }
    
    public LogNode GetParent(){
        if(_parentNode!=null){
            return _parentNode;
        }else{
            System.out.println("[WARN] LogNode parent not exist!");
            return null;
        }
    }
    
    public DataSession GetData(){
        return _nodeData;
    }
    
    public void AddChild(DataSession childData){
        LogNode childNode = new LogNode(childData);
        childNode.SetParent(this);
        _children.add(childNode);
    }

}
