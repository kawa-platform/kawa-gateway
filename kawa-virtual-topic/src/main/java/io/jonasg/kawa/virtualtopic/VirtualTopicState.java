package io.jonasg.kawa.virtualtopic;

import io.jonasg.kawa.core.GatewayContext;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/// State a virtual-topic transform keeps between a request and its response, to transform the
/// response. Lives on the request's [GatewayContext].
public final class VirtualTopicState {

    private final Map<String, String> physicalToVirtual = new LinkedHashMap<>();
    private final Set<String> hiddenTopics = new HashSet<>();
    private int fetchSessionId;
    private boolean offsetFetchAllTopics;
    private short apiVersion;
    private List<String> rejectedCreateTopics = List.of();
    private List<String> rejectedDeleteTopics = List.of();
    private Set<String> requestedTopics;

    /// The state of the request `context` belongs to, created on first use.
    public static VirtualTopicState from(GatewayContext context) {
        VirtualTopicState existing = context.state(VirtualTopicState.class);
        if (existing == null) {
            existing = new VirtualTopicState();
            context.state(VirtualTopicState.class, existing);
        }
        return existing;
    }

    /// Remembers that this request named `physical` as `virtual`, so the response can be renamed back.
    public void record(
            String physical,
            String virtual
    ) {
        physicalToVirtual.put(physical, virtual);
    }

    /// Keeps `virtual` out of this response: a physical topic is listed under each of its virtual topics, and the
    /// principal may not see this one. Recorded by authorization, which runs before the response is renamed.
    public void hide(String virtual) {
        hiddenTopics.add(virtual);
    }

    /// Whether `virtual` must be left out of this response, see [#hide].
    public boolean isHidden(String virtual) {
        return hiddenTopics.contains(virtual);
    }

    /// The name this request used for `physical`, or `null` when the request did not rename it.
    public String virtualFor(String physical) {
        return physicalToVirtual.get(physical);
    }

    /// Every rename this request recorded, physical name to virtual name, as an immutable copy.
    public Map<String, String> physicalToVirtual() {
        return Map.copyOf(physicalToVirtual);
    }

    /// The fetch session id the request sent: `0` when the client asked for a new session (or none).
    /// Compared with the response's session id to tell when the broker created a session.
    public int fetchSessionId() {
        return fetchSessionId;
    }

    /// Records the fetch session id the request sent.
    public void fetchSessionId(int sessionId) {
        this.fetchSessionId = sessionId;
    }

    /// Whether the OffsetFetch request asked for every topic the group committed (no topics listed). Its
    /// response then names topics the request never mentioned, so they are renamed from the configured
    /// virtual topics instead of from this request's renames.
    public boolean offsetFetchAllTopics() {
        return offsetFetchAllTopics;
    }

    /// Records whether the OffsetFetch request asked for every committed topic.
    public void offsetFetchAllTopics(boolean value) {
        this.offsetFetchAllTopics = value;
    }

    /// The request's wire version, needed to build a response the gateway answers itself
    /// (a [io.jonasg.kawa.core.ShortCircuitResult]).
    public short apiVersion() {
        return apiVersion;
    }

    /// Records the request's wire version.
    public void apiVersion(short version) {
        this.apiVersion = version;
    }

    /// The topics a CreateTopics request named that are virtual topics: they are not sent to the broker,
    /// and the response reports each of them as refused.
    public List<String> rejectedCreateTopics() {
        return rejectedCreateTopics;
    }

    /// Records the virtual topics a CreateTopics request named.
    public void rejectedCreateTopics(List<String> rejected) {
        this.rejectedCreateTopics = rejected;
    }

    /// The topics a DeleteTopics request named that are virtual topics: they are not sent to the broker,
    /// and the response reports each of them as refused.
    public List<String> rejectedDeleteTopics() {
        return rejectedDeleteTopics;
    }

    /// Records the virtual topics a DeleteTopics request named.
    public void rejectedDeleteTopics(List<String> rejected) {
        this.rejectedDeleteTopics = rejected;
    }

    /// Records the topics a Metadata request named, as the client named them.
    public void requestedTopics(Set<String> topics) {
        this.requestedTopics = Set.copyOf(topics);
    }

    /// Whether the response may list `topic`: the request named it, or named no topics (all topics).
    public boolean wasRequested(String topic) {
        return requestedTopics == null || requestedTopics.contains(topic);
    }
}
