/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.sql.planner;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import jakarta.validation.constraints.Min;

@DefunctConfig("compiler.interpreter-enabled")
public class CompilerConfig
{
    private int expressionCacheSize = 10_000;
    private boolean specializeAggregationLoops = true;

    @Min(0)
    public int getExpressionCacheSize()
    {
        return expressionCacheSize;
    }

    @Config("compiler.expression-cache-size")
    @ConfigDescription("Reuse compiled expressions across multiple queries")
    public CompilerConfig setExpressionCacheSize(int expressionCacheSize)
    {
        this.expressionCacheSize = expressionCacheSize;
        return this;
    }

    public boolean isSpecializeAggregationLoops()
    {
        return specializeAggregationLoops;
    }

    @Config("compiler.specialized-aggregation-loops")
    public CompilerConfig setSpecializeAggregationLoops(boolean specializeAggregationLoops)
    {
        this.specializeAggregationLoops = specializeAggregationLoops;
        return this;
    }
}
