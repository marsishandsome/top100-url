package mars.mapreduce;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * A simple implement of MinHeap
 */
public class MinHeap {
    private int size;
    private PriorityQueue<KeyValue> priorityQueue;

    public MinHeap(int size) {
        this.size = size;
        this.priorityQueue = new PriorityQueue<>(size + 1);
    }

    public void add(KeyValue keyValue) {
        this.priorityQueue.add(keyValue);
        if (this.priorityQueue.size() > this.size) {
            this.priorityQueue.poll();
        }
    }

    public List<KeyValue> getAll() {
        ArrayList<KeyValue> list = new ArrayList<>(this.size);
        priorityQueue.stream().forEach(list::add);
        return list;
    }
}
