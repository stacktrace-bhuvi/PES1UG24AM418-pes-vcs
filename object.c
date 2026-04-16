// object.c — Content-addressable object store

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/evp.h>

// ─── PROVIDED ──────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    unsigned int hash_len;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, id_out->hash, &hash_len);
    EVP_MD_CTX_free(ctx);
}

void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

// ─── IMPLEMENTATION ────────────────────────────────────────────
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    const char *type_str =
        (type == OBJ_BLOB) ? "blob" :
        (type == OBJ_TREE) ? "tree" : "commit";

    // Build header
    char header[64];
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len);
    if (header_len < 0 || header_len >= (int)sizeof(header) - 1)
        return -1;
    header[header_len++] = '\0';

    // Allocate full object
    size_t total_len = header_len + len;
    uint8_t *full = malloc(total_len);
    if (!full) return -1;

    memcpy(full, header, header_len);
    if (len > 0 && data) {
        memcpy(full + header_len, data, len);
    }

    // Compute hash
    compute_hash(full, total_len, id_out);

    // Deduplication
    if (object_exists(id_out)) {
        free(full);
        return 0;
    }

    // Build path
    char path[512];
    object_path(id_out, path, sizeof(path));

    // 🔥 FIX 1: ensure base objects dir exists
    mkdir(OBJECTS_DIR, 0755);

    // 🔥 FIX 2: create shard directory
    char dir[512];
    snprintf(dir, sizeof(dir), "%s/%.2s", OBJECTS_DIR, path + strlen(OBJECTS_DIR) + 1);
    mkdir(dir, 0755);

    // Temp file path
    char temp[512];
    if (snprintf(temp, sizeof(temp), "%s.tmp", path) >= (int)sizeof(temp)) {
        free(full);
        return -1;
    }

    int fd = open(temp, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) {
        free(full);
        return -1;
    }

    ssize_t written = write(fd, full, total_len);
    if (written < 0 || (size_t)written != total_len) {
        close(fd);
        free(full);
        return -1;
    }

    fsync(fd);
    close(fd);

    // Atomic rename
    if (rename(temp, path) != 0) {
        free(full);
        return -1;
    }

    free(full);
    return 0;
}

int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    char path[512];
    object_path(id, path, sizeof(path));

    FILE *fp = fopen(path, "rb");
    if (!fp) return -1;

    // Get file size
    if (fseek(fp, 0, SEEK_END) != 0) {
        fclose(fp);
        return -1;
    }

    long fsize = ftell(fp);
    if (fsize < 0) {
        fclose(fp);
        return -1;
    }
    rewind(fp);

    size_t size = (size_t)fsize;

    uint8_t *buf = malloc(size);
    if (!buf) {
        fclose(fp);
        return -1;
    }

    // SAFE fread
    if (fread(buf, 1, size, fp) != size) {
        free(buf);
        fclose(fp);
        return -1;
    }

    fclose(fp);

    // Verify integrity
    ObjectID check;
    compute_hash(buf, size, &check);
    if (memcmp(check.hash, id->hash, HASH_SIZE) != 0) {
        free(buf);
        return -1;
    }

    // Parse header
    char *null = memchr(buf, '\0', size);
    if (!null) {
        free(buf);
        return -1;
    }

    size_t header_len = (null - (char *)buf) + 1;
    size_t data_len = size - header_len;

    // Determine type
    if (strncmp((char *)buf, "blob", 4) == 0) *type_out = OBJ_BLOB;
    else if (strncmp((char *)buf, "tree", 4) == 0) *type_out = OBJ_TREE;
    else *type_out = OBJ_COMMIT;

    // Extract data
    void *data = malloc(data_len);
    if (!data) {
        free(buf);
        return -1;
    }

    memcpy(data, buf + header_len, data_len);

    *data_out = data;
    *len_out = data_len;

    free(buf);
    return 0;
}
