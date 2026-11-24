/*
 * Copyright 2024-2025, Seqera Labs
 *
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
package nextflow.lsp.services.script.dag;

import java.util.List;
import java.util.Map;

import com.google.gson.JsonPrimitive;

import nextflow.lsp.TestServer;
import nextflow.lsp.TestWorkspace;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static nextflow.lsp.TestUtils.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Specifies the JSON structure of a script returned by the
 * nextflow.server.previewDagJson command.
 */
@ExtendWith(TestWorkspace.class)
class PreviewDagJsonTest {

    static Map<?,?> previewDagJson(TestServer server, String path) {
        server.settle();
        var response = (Map<?,?>) executeCommand(server, "nextflow.server.previewDagJson", List.of(new JsonPrimitive(getUri(path))));
        assertThat(response).isNotNull();
        assertThat(response.get("error")).isNull();
        return (Map<?,?>) response.get("result");
    }

    static Map<?,?> workflow(Map<?,?> result, String name) {
        return ((List<?>) result.get("workflows")).stream()
            .<Map<?,?>>map(w -> (Map<?,?>) w)
            .filter(w -> name == null ? w.get("name") == null : name.equals(w.get("name")))
            .findFirst()
            .orElseThrow();
    }

    static List<Map<?,?>> nodes(Map<?,?> workflow, String label) {
        var graph = (Map<?,?>) workflow.get("graph");
        return ((List<?>) graph.get("nodes")).stream()
            .<Map<?,?>>map(n -> (Map<?,?>) n)
            .filter(n -> label.equals(n.get("label")))
            .toList();
    }

    static Map<?,?> node(Map<?,?> workflow, String label) {
        var result = nodes(workflow, label);
        assertThat(result).as("nodes labelled '%s'", label).hasSize(1);
        return result.get(0);
    }

    @Test
    void renderProcessesWorkflowsAndExpandedCalls() {
        var server = TestServer.start();
        server.open("main.nf", """
            process HELLO {
                input:
                val x

                output:
                val x

                script:
                "echo $x"
            }

            workflow FOO {
                take:
                a

                main:
                HELLO(a)

                emit:
                out = HELLO.out
            }

            workflow {
                x = channel.of(1)
                FOO(x)
            }
            """);
        var result = previewDagJson(server, "main.nf");

        var processes = (List<?>) result.get("processes");
        assertThat(processes).hasSize(1);
        var hello = (Map<?,?>) processes.get(0);
        assertThat(hello.get("name")).isEqualTo("HELLO");
        assertThat((String) hello.get("code")).startsWith("process HELLO {").endsWith("}");

        var foo = workflow(result, "FOO");
        assertThat((String) foo.get("code")).startsWith("workflow FOO {");
        var fooGraph = (Map<?,?>) foo.get("graph");
        assertThat(((Map<Object,Object>) fooGraph.get("inputs")).keySet()).containsExactly("a");
        assertThat(((Map<Object,Object>) fooGraph.get("outputs")).keySet()).containsExactly("out");
        var helloCall = node(foo, "HELLO");
        assertThat(helloCall.get("kind")).isEqualTo("process");
        assertThat(helloCall.get("definition")).isEqualTo("HELLO");

        var entry = workflow(result, null);
        assertThat(node(entry, "take:a").get("type")).isEqualTo("INPUT");
        assertThat(node(entry, "emit:out").get("type")).isEqualTo("OUTPUT");
        var fooCall = node(entry, "FOO");
        assertThat(fooCall.get("kind")).isEqualTo("workflow");
        assertThat(fooCall.get("definition")).isEqualTo("FOO");
        assertThat((String) fooCall.get("uri")).endsWith("main.nf");
    }

    @Test
    void renderConditionsAndBranches() {
        var server = TestServer.start();
        server.open("main.nf", """
            process A {
                output:
                val 1

                script:
                "true"
            }

            process B {
                output:
                val 2

                script:
                "true"
            }

            workflow {
                if( params.flag ) {
                    A()
                }
                else {
                    B()
                }
            }
            """);
        var entry = workflow(previewDagJson(server, "main.nf"), null);

        var control = node(entry, "params.flag");
        assertThat(control.get("type")).isEqualTo("CONTROL");

        var root = (Map<?,?>) ((Map<?,?>) entry.get("graph")).get("subgraph");
        var branches = ((List<?>) root.get("subgraphs")).stream()
            .<Map<?,?>>map(s -> (Map<?,?>) s)
            .filter(s -> control.get("id").equals(s.get("pred")))
            .toList();
        assertThat(branches).hasSize(2);
        assertThat(branches.get(0).get("branch")).isEqualTo("if");
        assertThat((List<Object>) branches.get(0).get("nodes")).contains(node(entry, "A").get("id"));
        assertThat(branches.get(1).get("branch")).isEqualTo("else");
        assertThat((List<Object>) branches.get(1).get("nodes")).contains(node(entry, "B").get("id"));
    }

    @Test
    void resolveAliasedIncludes() {
        var server = TestServer.start();
        server.open("modules/hello.nf", """
            process HELLO {
                output:
                val 1

                script:
                "true"
            }
            """);
        server.open("main.nf", """
            include { HELLO as GREET } from './modules/hello.nf'

            workflow {
                GREET()
            }
            """);
        var entry = workflow(previewDagJson(server, "main.nf"), null);

        var call = node(entry, "GREET");
        assertThat(call.get("kind")).isEqualTo("process");
        assertThat(call.get("definition")).isEqualTo("HELLO");
        assertThat((String) call.get("uri")).endsWith("modules/hello.nf");
    }

}
