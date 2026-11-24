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

import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import nextflow.lsp.ast.ScriptView;
import org.codehaus.groovy.ast.ASTNode;
import org.codehaus.groovy.control.SourceUnit;

/**
 * Render the dataflow graphs of a script, and the source code of its
 * processes and workflows, as a JSON-like structure of maps and lists.
 *
 * Unlike the Mermaid diagram, this keeps everything a client needs to
 * rebuild the structure of a pipeline, e.g. to draw a metro map:
 * workflow calls expanded into take and emit nodes, the kind of
 * definition that each call refers to, the text of each condition, and
 * the branch (if/else) of each conditional subgraph.
 */
public class JsonRenderer {

    private final SourceUnit sourceUnit;

    private final ScriptView ast;

    public JsonRenderer(SourceUnit sourceUnit, ScriptView ast) {
        this.sourceUnit = sourceUnit;
        this.ast = ast;
    }

    /**
     * Render the processes and workflows defined in a script. The
     * visitor must have been run with workflow calls expanded.
     *
     * @param uri
     * @param visitor
     */
    public Map<String,Object> render(URI uri, DataflowVisitor visitor) {
        var processes = new ArrayList<Map<String,Object>>();
        for( var pn : ast.getProcessNodes(uri) ) {
            var entry = new LinkedHashMap<String,Object>();
            entry.put("name", pn.getName());
            entry.put("code", sourceText(pn));
            processes.add(entry);
        }

        var workflows = new ArrayList<Map<String,Object>>();
        for( var wn : ast.getWorkflowNodes(uri) ) {
            var graph = visitor.getGraph(wn.isEntry() ? "<entry>" : wn.getName());
            var entry = new LinkedHashMap<String,Object>();
            entry.put("name", wn.isEntry() ? null : wn.getName());
            entry.put("code", sourceText(wn));
            entry.put("graph", graph != null ? renderGraph(graph) : null);
            workflows.add(entry);
        }

        var result = new LinkedHashMap<String,Object>();
        result.put("processes", processes);
        result.put("workflows", workflows);
        return result;
    }

    private Map<String,Object> renderGraph(Graph graph) {
        var nodes = graph.nodes.values().stream()
            .sorted(Comparator.comparingInt(n -> n.id))
            .map(this::renderNode)
            .toList();

        // the subgraphs of a control node are pushed in order: the if
        // (or true) branch first, then the else (or false) branch
        var branches = new HashMap<Subgraph,String>();
        var seen = new HashMap<Node,Integer>();
        collectBranches(graph.peekSubgraph(), branches, seen);

        var result = new LinkedHashMap<String,Object>();
        result.put("inputs", renderIds(graph.inputs));
        result.put("outputs", renderIds(graph.outputs));
        result.put("nodes", nodes);
        result.put("subgraph", renderSubgraph(graph.peekSubgraph(), branches));
        return result;
    }

    private static void collectBranches(Subgraph subgraph, Map<Subgraph,String> branches, Map<Node,Integer> seen) {
        for( var s : subgraph.subgraphs ) {
            if( s.pred != null ) {
                var index = seen.merge(s.pred, 1, Integer::sum);
                branches.put(s, index == 1 ? "if" : "else");
            }
            collectBranches(s, branches, seen);
        }
    }

    private static Map<String,Object> renderIds(Map<String,Node> nodes) {
        var result = new TreeMap<String,Object>();
        nodes.forEach((name, dn) -> {
            if( dn != null )
                result.put(name, dn.id);
        });
        return result;
    }

    private Map<String,Object> renderNode(Node dn) {
        var result = new LinkedHashMap<String,Object>();
        result.put("id", dn.id);
        result.put("label", dn.label);
        result.put("type", dn.type.name());
        result.put("uri", dn.uri != null ? dn.uri.toString() : null);
        result.put("kind", dn.kind);
        result.put("definition", dn.definition);
        result.put("verbose", dn.verbose);
        result.put("preds", dn.preds.stream().map(p -> p.id).sorted().toList());
        return result;
    }

    private Map<String,Object> renderSubgraph(Subgraph subgraph, Map<Subgraph,String> branches) {
        var result = new LinkedHashMap<String,Object>();
        result.put("id", subgraph.id);
        result.put("pred", subgraph.pred != null ? subgraph.pred.id : null);
        result.put("branch", branches.get(subgraph));
        result.put("nodes", subgraph.nodes.stream().map(n -> n.id).sorted().toList());
        result.put("subgraphs", subgraph.subgraphs.stream().map(s -> renderSubgraph(s, branches)).toList());
        return result;
    }

    /**
     * Get the source text of a declaration from the source unit, which
     * holds the text that was compiled. Returns "" if the text can't
     * be resolved.
     *
     * @param node
     */
    private String sourceText(ASTNode node) {
        var first = node.getLineNumber();
        var last = node.getLastLineNumber();
        if( first == -1 || last < first )
            return "";
        var source = sourceUnit.getSource();
        var builder = new StringBuilder();
        for( int i = first; i <= last; i++ ) {
            var line = source.getLine(i, null);
            if( line == null )
                return "";
            var begin = i == first ? node.getColumnNumber() - 1 : 0;
            var end = i == last ? node.getLastColumnNumber() - 1 : line.length();
            if( begin < 0 || end > line.length() || begin > end )
                return "";
            if( i > first )
                builder.append('\n');
            builder.append(line, begin, end);
        }
        return builder.toString();
    }

}
