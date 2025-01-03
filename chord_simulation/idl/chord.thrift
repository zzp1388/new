namespace py chord

service ChordNode {
    KeyValueResult lookup(1: string key),
    Node find_successor(1: i32 key_id),
    Node find_finger(1: i32 key_id),
    KeyValueResult put(1: string key, 2: string value),
    KeyValueResult do_put(1: string key, 2: string value, 3: string place),
    void join(1: Node node),
    void notify(1: Node node),
    Node get_predecessor(),
    Node get_successor(),
    i32 get_id(),
    map<string, string> get_all_data(1: string place),
    void check_and_clean_data(),
    void update_successor_kv_store(),
    void update_predecessor_kv_store(),
    void leave_network(),
    void update_predecessor(1: Node predecessor),
    void update_successor(1: Node successor),
    void pause_stability_tests(),
    void resume_stability_tests(),
    Node check_predecessor()
}

enum KVStatus {
    VALID, NOT_FOUND
}

struct KeyValueResult {
    1: string key,
    2: string value,
    3: i32 node_id,
    4: KVStatus status,
}

struct Node {
    1: i32 node_id,
    2: string address,
    3: i32 port,
    4: bool valid,
}

