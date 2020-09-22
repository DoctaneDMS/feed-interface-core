/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl;

import java.net.URI;

/** Resolve some remote object.
 *
 * @author jonathan essex
 * @param <T> Type of remote object
 */
@FunctionalInterface
public interface Resolver<T> {
    T resolve(URI endpoint);   
}
