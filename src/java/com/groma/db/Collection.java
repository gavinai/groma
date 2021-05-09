package com.groma.db;

import java.util.ArrayList;
import java.util.Iterator;

public class Collection<T> implements Iterable<T> {

    private ArrayList<T> _inner;

    public Collection(int capacity) {
        this._inner = new ArrayList<T>(capacity);
    }

    public void add(T item) {
        this._inner.add(item);
    }

    public void addAll(java.util.Collection<T> list) {
        this._inner.addAll(list);
    }

    public void addAll(T[] list) {
        this._inner.ensureCapacity(this._inner.size() + list.length);
        for (int i = 0; i < list.length; i++) {
            this._inner.add(list[i]);
        }
    }

    public T get(int i) {
        return this._inner.get(i);
    }

    public int size() {
        return this._inner.size();
    }

    @Override
    public Iterator<T> iterator() {
        return this._inner.iterator();
    }
}