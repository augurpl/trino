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
package io.trino.plugin.elasticsearch;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.elasticsearch.client.OpensearchClient;
import io.trino.plugin.elasticsearch.ptf.RawQuery;
import io.trino.spi.function.table.ConnectorTableFunction;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.elasticsearch.OpensearchConfig.Security.AWS;
import static io.trino.plugin.elasticsearch.OpensearchConfig.Security.PASSWORD;
import static java.util.function.Predicate.isEqual;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class OpensearchConnectorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(OpensearchConnector.class).in(Scopes.SINGLETON);
        binder.bind(OpensearchMetadata.class).in(Scopes.SINGLETON);
        binder.bind(OpensearchSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(OpensearchPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(OpensearchClient.class).in(Scopes.SINGLETON);
        binder.bind(NodesSystemTable.class).in(Scopes.SINGLETON);

        newExporter(binder).export(OpensearchClient.class).withGeneratedName();

        configBinder(binder).bindConfig(OpensearchConfig.class);

        newOptionalBinder(binder, AwsSecurityConfig.class);
        newOptionalBinder(binder, PasswordConfig.class);

        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(RawQuery.class).in(Scopes.SINGLETON);

        install(conditionalModule(
                OpensearchConfig.class,
                config -> config.getSecurity()
                        .filter(isEqual(AWS))
                        .isPresent(),
                conditionalBinder -> configBinder(conditionalBinder).bindConfig(AwsSecurityConfig.class)));

        install(conditionalModule(
                OpensearchConfig.class,
                config -> config.getSecurity()
                        .filter(isEqual(PASSWORD))
                        .isPresent(),
                conditionalBinder -> configBinder(conditionalBinder).bindConfig(PasswordConfig.class)));
    }
}
