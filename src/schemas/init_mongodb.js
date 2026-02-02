print('Initializing GeoVersion database...');
db.createCollection('bpo_cas', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['hash', 'geometry', 'attributes'],
            properties: {
                hash: {
                    bsonType: 'string',
                    description: 'SHA-256 hash of the BPO content'
                },
                geometry: {
                    bsonType: 'object',
                    description: 'GeoJSON geometry object'
                },
                attributes: {
                    bsonType: 'object',
                    description: 'BPO attributes'
                },
                created_at: {
                    bsonType: 'date',
                    description: 'Creation timestamp'
                }
            }
        }
    }
});

db.createCollection('situations', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['situation_id', 'name', 'created_at'],
            properties: {
                situation_id: {
                    bsonType: 'string',
                    description: 'Unique situation identifier'
                },
                name: {
                    bsonType: 'string',
                    description: 'Situation name'
                },
                description: {
                    bsonType: 'string',
                    description: 'Situation description'
                },
                created_at: {
                    bsonType: 'date',
                    description: 'Creation timestamp'
                },
                updated_at: {
                    bsonType: 'date',
                    description: 'Last update timestamp'
                }
            }
        }
    }
});

db.createCollection('situation_versions', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['version_id', 'situation_id', 'created_at'],
            properties: {
                version_id: {
                    bsonType: 'string',
                    description: 'Unique version identifier'
                },
                situation_id: {
                    bsonType: 'string',
                    description: 'Reference to situation'
                },
                parent_version_ids: {
                    bsonType: 'array',
                    items: {
                        bsonType: 'string'
                    },
                    description: 'Parent version IDs for merge support'
                },
                commit_message: {
                    bsonType: 'string',
                    description: 'Commit message'
                },
                author: {
                    bsonType: 'string',
                    description: 'Version author'
                },
                created_at: {
                    bsonType: 'date',
                    description: 'Creation timestamp'
                },
                bpo_refs: {
                    bsonType: 'array',
                    items: {
                        bsonType: 'string'
                    },
                    description: 'Array of BPO hashes in this version'
                }
            }
        }
    }
});

db.createCollection('version_deltas', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['delta_id', 'from_version_id', 'to_version_id'],
            properties: {
                delta_id: {
                    bsonType: 'string',
                    description: 'Unique delta identifier'
                },
                from_version_id: {
                    bsonType: 'string',
                    description: 'Source version ID'
                },
                to_version_id: {
                    bsonType: 'string',
                    description: 'Target version ID'
                },
                added_bpos: {
                    bsonType: 'array',
                    items: {
                        bsonType: 'string'
                    },
                    description: 'Array of added BPO hashes'
                },
                removed_bpos: {
                    bsonType: 'array',
                    items: {
                        bsonType: 'string'
                    },
                    description: 'Array of removed BPO hashes'
                },
                modified_bpos: {
                    bsonType: 'array',
                    items: {
                        bsonType: 'object',
                        properties: {
                            old_hash: { bsonType: 'string' },
                            new_hash: { bsonType: 'string' }
                        }
                    },
                    description: 'Array of modified BPOs'
                }
            }
        }
    }
});

print('Collections created successfully.');

print('Creating geospatial indexes...');

// Index on BPO CAS collection for geometry
db.bpo_cas.createIndex(
    { 'geometry': '2dsphere' },
    { name: 'geometry_2dsphere_idx' }
);

// Index on hash for fast CAS lookups
db.bpo_cas.createIndex(
    { 'hash': 1 },
    { name: 'hash_idx', unique: true }
);

// Indexes for situations
db.situations.createIndex(
    { 'situation_id': 1 },
    { name: 'situation_id_idx', unique: true }
);

// Indexes for situation versions
db.situation_versions.createIndex(
    { 'version_id': 1 },
    { name: 'version_id_idx', unique: true }
);

db.situation_versions.createIndex(
    { 'situation_id': 1, 'created_at': -1 },
    { name: 'situation_versions_lookup_idx' }
);

// Index for version deltas
db.version_deltas.createIndex(
    { 'delta_id': 1 },
    { name: 'delta_id_idx', unique: true }
);

db.version_deltas.createIndex(
    { 'from_version_id': 1, 'to_version_id': 1 },
    { name: 'delta_lookup_idx' }
);

print('Geospatial indexes created successfully.');

// Display collection stats
print('\nDatabase initialization complete!');
print('Collections:');
db.getCollectionNames().forEach(function(collection) {
    print('  - ' + collection);
});

print('\nIndexes:');
db.bpo_cas.getIndexes().forEach(function(index) {
    print('  bpo_cas.' + index.name);
});
