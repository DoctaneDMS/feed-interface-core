/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl.buffer;

import com.softwareplumbers.common.pipedstream.InputStreamSupplier;

/**
 *
 * @author jonathan
 */
class MessageOverflow extends Exception {
    
    private final InputStreamSupplier overflow;
    private final int size;

    public MessageOverflow(int size, InputStreamSupplier overflow) {
        super("message overflow");
        this.overflow = overflow;
        this.size = size;
    }

    public InputStreamSupplier getOverflow() {
        return overflow;
    }

    public int getSize() {
        return size;
    }
    
}
