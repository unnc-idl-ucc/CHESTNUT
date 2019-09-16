package com.chestnut.runtime.dal.ma;

import java.util.ArrayList;
import java.util.List;

public class BinarySearchTree {

    private BinaryNode _treeRoot, _branchRoot;
    private int _treeSize = 0, _sortAmount = 0;
    private List<Integer> _sortTreeArr;
    private String _sortOrder;
    
    public BinarySearchTree(BinaryNode treeRoot, int sortAmount, String sortOrder) {
        _treeRoot = treeRoot;
        _sortAmount = sortAmount;
        _sortOrder = sortOrder;
        _branchRoot = treeRoot;
        _treeSize = 1;
        _sortTreeArr = new ArrayList<Integer>();
    }
    
    public int GetTreeSize() {
        return _treeSize;
    }
    
    public List<Integer> GetSortedArr(){
        Iterate(_treeRoot);
        return _sortTreeArr;
    }
    
    public List<Integer> GetSortedArrByAmount(){
        switch(_sortOrder) {
        case "DESC":
            IterateByAmountDESC(_treeRoot);
            break;
            
        case "ASC":
            IterateByAmountASC(_treeRoot);
            break;
        }
        
        return _sortTreeArr;
    }
    
    public void AddNewNode(Double nodeValue, int nodeIndex) {
        if(nodeValue<_branchRoot.GetValue()) {
            if(_branchRoot.GetLeftChild()!=null) {
                _branchRoot = _branchRoot.GetLeftChild();
                AddNewNode(nodeValue, nodeIndex);
                _branchRoot = _treeRoot;
            }else {
                _branchRoot.SetLeftChild(new BinaryNode(nodeValue, nodeIndex, null, null));
                _treeSize++;
            }
        }else {
            if(_branchRoot.GetRightChild()!=null) {
                _branchRoot = _branchRoot.GetRightChild();
                AddNewNode(nodeValue, nodeIndex);
                _branchRoot = _treeRoot;
            }else {
                _branchRoot.SetRightChild(new BinaryNode(nodeValue, nodeIndex, null, null));
                _treeSize++;
            }
        }
        
    }
    
    private void Iterate(BinaryNode branchNode) {
        
        if(branchNode.GetLeftChild()!=null) {
            Iterate(branchNode.GetLeftChild());
        }
        _sortTreeArr.add(branchNode.GetNodeIndex());
        //System.out.println("[Tracing] BinarySearchTree.Iterate(), branchNode value is " + branchNode.GetValue() + ", original index is " + branchNode.GetNodeIndex());
        if(branchNode.GetRightChild()!=null) {
            Iterate(branchNode.GetRightChild());
        }
    }
    
    private void IterateByAmountASC(BinaryNode branchNode) {
        if(branchNode.GetLeftChild()!=null&&_sortTreeArr.size()<=_sortAmount) {
            IterateByAmountASC(branchNode.GetLeftChild());
        }
        _sortTreeArr.add(branchNode.GetNodeIndex());
        //System.out.println("[Tracing] BinarySearchTree.Iterate(), branchNode value is " + branchNode.GetValue() + ", original index is " + branchNode.GetNodeIndex());
        if(branchNode.GetRightChild()!=null&&_sortTreeArr.size()<=_sortAmount) {
            IterateByAmountASC(branchNode.GetRightChild());
        }
    }
    
    private void IterateByAmountDESC(BinaryNode branchNode) {
        if(branchNode.GetRightChild()!=null&&_sortTreeArr.size()<=_sortAmount) {
            IterateByAmountDESC(branchNode.GetRightChild());
        }
        _sortTreeArr.add(branchNode.GetNodeIndex());
        //System.out.println("[Tracing] BinarySearchTree.Iterate(), branchNode value is " + branchNode.GetValue() + ", original index is " + branchNode.GetNodeIndex());
        if(branchNode.GetLeftChild()!=null&&_sortTreeArr.size()<=_sortAmount) {
            IterateByAmountDESC(branchNode.GetLeftChild());
        }
    }
    
    public void PrintTreeArr() {
        for(int i=0; i<_sortTreeArr.size(); i++) {
            System.out.println("[Tracing] BinarySearchTree.Iterate(), _sortTreeArr-" + i + " is " + _sortTreeArr.get(i));
        }
        
    }
}
