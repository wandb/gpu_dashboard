QUERY = """\
query GetGpuInfoForProject($project: String!, $entity: String!, $first: Int!, $cursor: String!) {
    project(name: $project, entityName: $entity) {
        name
        runs(first: $first, after: $cursor) {
            edges {
                cursor
                node {
                    name
                    user {
                        username
                    }
                    computeSeconds
                    createdAt
                    updatedAt
                    state
                    tags
                    systemMetrics
                    runInfo {
                        gpuCount
                        gpu
                    }
                }
            }
        }
    }
}\
"""