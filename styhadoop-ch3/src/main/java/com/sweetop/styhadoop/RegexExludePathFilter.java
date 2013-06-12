package com.sweetop.styhadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * Created with IntelliJ IDEA.
 * User: lastsweetop
 * Date: 13-6-3
 * Time: 下午2:49
 * To change this template use File | Settings | File Templates.
 */
public class RegexExludePathFilter implements PathFilter {

    private final String regex;

    public RegexExludePathFilter(String regex) {
        this.regex = regex;
    }

    @Override
    public boolean accept(Path path) {
        return !path.toString().matches(regex);
    }
}
