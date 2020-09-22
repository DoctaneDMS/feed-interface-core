/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl;

import java.net.URI;
import java.util.Optional;
import javax.json.JsonObject;

/** Resolve some remote object.
 *
 * @author jonathan essex
 * @param <T> Type of remote object
 */
@FunctionalInterface
public interface Resolver<T> {
    Optional<T> resolve(URI endpoint, JsonObject credentials);   
}
