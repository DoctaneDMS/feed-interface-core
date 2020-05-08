/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl.buffer;

/**
 *
 * @author jonathan
 */
class HeaderOverflow extends Exception {
    
    private final int size;

    public HeaderOverflow(int size) {
        super("header overflow");
        this.size = size;
    }

    public int getSize() {
        return size;
    }
    
}
