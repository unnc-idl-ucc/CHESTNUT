package com.chestnut.runtime.dal.ma;

public class BinaryNode {

    private Double _nodeValue;
    private int _nodeIndex;
    private BinaryNode _leftChild;
    private BinaryNode _rightChild;
    
    public BinaryNode(Double nodeValue, int nodeIndex, BinaryNode leftChild, BinaryNode rightChild) {
        _nodeValue = nodeValue;
        _nodeIndex = nodeIndex;
        _leftChild = leftChild;
        _rightChild = rightChild;
    }
    
    public BinaryNode GetLeftChild() {
        return _leftChild;
    }
    
    public void SetLeftChild(BinaryNode child) {
        _leftChild = child;
    }
    
    public BinaryNode GetRightChild() {
        return _rightChild;
    }
    
    public void SetRightChild(BinaryNode child) {
        _rightChild = child;
    }
    
    public Double GetValue() {
        return _nodeValue;
    }
    
    public void SetValue(Double value) {
        _nodeValue = value;
    }
    
    public int GetNodeIndex() {
        return _nodeIndex;
    }
    
    public void SetNodeIndex(int nodeIndex) {
        _nodeIndex = nodeIndex;
    }
}
