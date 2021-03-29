package com.example.toydb.common;

import com.example.toydb.Data;
import org.junit.Test;

import java.util.List;

public class RBTreeTest {

    @Test
    public void insertTest(){
        RBTree tree = new RBTree();

        for(int i=0; i<= 50; i++){
            tree.insert(Integer.toString(i),"Value"+i);
        }

        List<RBTree.Node> treeElements = tree.getAllNodes();
        System.out.println("Tree size : " + treeElements.size());

        treeElements.forEach(System.out::println);

        Pair<Data, Data> predSuccPair1 = tree.getInorderSuccessorPredecessor("Key8.5");
        Pair<Data, Data> predSuccPair2 = tree.getInorderSuccessorPredecessor("Key0");
        Pair<Data, Data> predSuccPair3 = tree.getInorderSuccessorPredecessor("Key91");

        System.out.println("Test ended");
    }
}
