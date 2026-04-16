// index.c — Staging area implementation

#include "index.h"
#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

// Forward declaration
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);

// ─── PROVIDED ────────────────────────────────────────────────────────────────

IndexEntry* index_find(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0)
            return &index->entries[i];
    }
    return NULL;
}

int index_remove(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0) {
            int remaining = index->count - i - 1;
            if (remaining > 0)
                memmove(&index->entries[i], &index->entries[i + 1],
                        remaining * sizeof(IndexEntry));
            index->count--;
            return index_save(index);
        }
    }
    fprintf(stderr, "error: '%s' is not in the index\n", path);
    return -1;
}

int index_status(const Index *index) {
    printf("Staged changes:\n");
    int staged_count = 0;

    for (int i = 0; i < index->count; i++) {
        printf("  staged:     %s\n", index->entries[i].path);
        staged_count++;
    }
    if (staged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Unstaged changes:\n");
    int unstaged_count = 0;
    for (int i = 0; i < index->count; i++) {
        struct stat st;
        if (stat(index->entries[i].path, &st) != 0) {
            printf("  deleted:    %s\n", index->entries[i].path);
            unstaged_count++;
        } else {
            if (st.st_mtime != (time_t)index->entries[i].mtime_sec ||
                st.st_size != (off_t)index->entries[i].size) {
                printf("  modified:   %s\n", index->entries[i].path);
                unstaged_count++;
            }
        }
    }
    if (unstaged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Untracked files:\n");
    int untracked_count = 0;
    DIR *dir = opendir(".");
    if (dir) {
        struct dirent *ent;
        while ((ent = readdir(dir)) != NULL) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
            if (strcmp(ent->d_name, ".pes") == 0) continue;
            if (strcmp(ent->d_name, "pes") == 0) continue;
            if (strstr(ent->d_name, ".o") != NULL) continue;

            int is_tracked = 0;
            for (int i = 0; i < index->count; i++) {
                if (strcmp(index->entries[i].path, ent->d_name) == 0) {
                    is_tracked = 1;
                    break;
                }
            }

            if (!is_tracked) {
                struct stat st;
                stat(ent->d_name, &st);
                if (S_ISREG(st.st_mode)) {
                    printf("  untracked:  %s\n", ent->d_name);
                    untracked_count++;
                }
            }
        }
        closedir(dir);
    }
    if (untracked_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    return 0;
}

// ─── IMPLEMENTATION ──────────────────────────────────────────────────────────

// Load index
int index_load(Index *index) {
    index->count = 0;  // 🔥 CRITICAL FIX

    FILE *fp = fopen(".pes/index", "r");
    if (!fp) return 0; // empty index is OK

    while (index->count < MAX_INDEX_ENTRIES) {
        IndexEntry *e = &index->entries[index->count];

        char hex[HASH_HEX_SIZE + 1];

        int ret = fscanf(fp, "%o %64s %ld %u %s\n",
                         &e->mode,
                         hex,
                         &e->mtime_sec,
                         &e->size,
                         e->path);

        if (ret != 5) break;

        if (hex_to_hash(hex, &e->hash) != 0) {
            fclose(fp);
            return -1;
        }

        index->count++;
    }

    fclose(fp);
    return 0;
}
// Sorting helper
static int compare_entries(const void *a, const void *b) {
    return strcmp(((const IndexEntry *)a)->path,
                  ((const IndexEntry *)b)->path);
}

// Save index


int index_save(const Index *index) {
    FILE *fp = fopen(".pes/index.tmp", "w");
    if (!fp) return -1;

    // 🔥 make a SAFE copy
    IndexEntry *sorted = malloc(sizeof(IndexEntry) * index->count);
    if (!sorted) {
        fclose(fp);
        return -1;
    }

    memcpy(sorted, index->entries, sizeof(IndexEntry) * index->count);

    qsort(sorted, index->count, sizeof(IndexEntry), compare_entries);

    for (int i = 0; i < index->count; i++) {
        char hex[HASH_HEX_SIZE + 1];
        hash_to_hex(&sorted[i].hash, hex);

        fprintf(fp, "%o %s %ld %u %s\n",
                sorted[i].mode,
                hex,
                sorted[i].mtime_sec,
                sorted[i].size,
                sorted[i].path);
    }

    fflush(fp);
    fsync(fileno(fp));
    fclose(fp);

    rename(".pes/index.tmp", ".pes/index");

    free(sorted);
    return 0;
}

// Add file to index
int index_add(Index *index, const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) {
        perror("stat");
        return -1;
    }

    // ✅ Reject non-regular files
    if (!S_ISREG(st.st_mode)) {
        fprintf(stderr, "Skipping non-regular file: %s\n", path);
        return -1;
    }

    FILE *fp = fopen(path, "rb");
    if (!fp) {
        perror("fopen");
        return -1;
    }

    size_t size = (size_t)st.st_size;

    // ✅ Handle empty file safely
    void *data = NULL;
    if (size > 0) {
        data = malloc(size);
        if (!data) {
            fclose(fp);
            return -1;
        }

        if (fread(data, 1, size, fp) != size) {
            perror("fread");
            free(data);
            fclose(fp);
            return -1;
        }
    }

    fclose(fp);

    ObjectID id;
    if (object_write(OBJ_BLOB, data, size, &id) != 0) {
        free(data);
        return -1;
    }

    free(data);

    IndexEntry *e = index_find(index, path);
    if (!e) {
        if (index->count >= MAX_INDEX_ENTRIES) {
            fprintf(stderr, "Index full\n");
            return -1;
        }
        e = &index->entries[index->count++];
    }

    e->mode = (st.st_mode & S_IXUSR) ? 0100755 : 0100644;
    e->hash = id;
    e->mtime_sec = st.st_mtime;
    e->size = (uint32_t)size;

    // ✅ SAFE STRING COPY
    strncpy(e->path, path, sizeof(e->path) - 1);
    e->path[sizeof(e->path) - 1] = '\0';

    return index_save(index);
}

