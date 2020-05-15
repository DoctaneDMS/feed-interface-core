/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

/** Simple feed interface.
 *
 * @author jonathan
 */
public interface Feed {
    
    /** Get the name of the feed.
     * 
     * @return the feed name.
     */
    FeedPath getName();
    
    /** Get the id of the feed.
     * 
     * @return the feed id.
     */
    String getId();
}
