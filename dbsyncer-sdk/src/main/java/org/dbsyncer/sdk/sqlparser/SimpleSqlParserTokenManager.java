/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.sqlparser;

import net.sf.jsqlparser.parser.CCJSqlParserTokenManager;
import net.sf.jsqlparser.parser.SimpleCharStream;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-03-02 16:07
 */
public class SimpleSqlParserTokenManager extends CCJSqlParserTokenManager {

    int curLexState = 0;
    int defaultLexState = 0;

    public SimpleSqlParserTokenManager(SimpleCharStream stream) {
        super(stream);
    }

    public SimpleSqlParserTokenManager(SimpleCharStream stream, int lexState) {
        super(stream, lexState);
    }

    @Override
    public void ReInit(SimpleCharStream stream) {
        curLexState = defaultLexState;
        super.ReInit(stream);
    }

    /**
     * Switch to specified lex state.
     */
    public void SwitchTo(int lexState) {
        curLexState = lexState;
        super.SwitchTo(lexState);
    }
}
