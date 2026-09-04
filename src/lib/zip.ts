import { crc32 } from "node:zlib";
import { Stream } from "effect";

export interface ZipEntry<E> {
  readonly name: string;
  readonly content: Stream.Stream<Uint8Array, E>;
}

const SIG_LOCAL = 0x04034b50;
const SIG_DESCRIPTOR = 0x08074b50;
const SIG_CENTRAL = 0x02014b50;
const SIG_EOCD = 0x06054b50;
// Bit 3: crc/sizes trail the data (we only know them after streaming it).
// Bit 11: file names are UTF-8.
const FLAGS = 0x0808;
const VERSION = 20;

const dosDateTime = (d: Date) => ({
  time: (d.getHours() << 11) | (d.getMinutes() << 5) | (d.getSeconds() >> 1),
  date: ((d.getFullYear() - 1980) << 9) | ((d.getMonth() + 1) << 5) | d.getDate(),
});

const localHeader = (name: Buffer, time: number, date: number): Uint8Array => {
  const b = Buffer.alloc(30 + name.length);
  b.writeUInt32LE(SIG_LOCAL, 0);
  b.writeUInt16LE(VERSION, 4);
  b.writeUInt16LE(FLAGS, 6);
  b.writeUInt16LE(time, 10);
  b.writeUInt16LE(date, 12);
  b.writeUInt16LE(name.length, 26);
  name.copy(b, 30);
  return b;
};

const descriptor = (crc: number, size: number): Uint8Array => {
  const b = Buffer.alloc(16);
  b.writeUInt32LE(SIG_DESCRIPTOR, 0);
  b.writeUInt32LE(crc, 4);
  b.writeUInt32LE(size, 8);
  b.writeUInt32LE(size, 12);
  return b;
};

const centralHeader = (
  name: Buffer,
  time: number,
  date: number,
  crc: number,
  size: number,
  offset: number,
): Buffer => {
  const b = Buffer.alloc(46 + name.length);
  b.writeUInt32LE(SIG_CENTRAL, 0);
  b.writeUInt16LE(VERSION, 4);
  b.writeUInt16LE(VERSION, 6);
  b.writeUInt16LE(FLAGS, 8);
  b.writeUInt16LE(time, 12);
  b.writeUInt16LE(date, 14);
  b.writeUInt32LE(crc, 16);
  b.writeUInt32LE(size, 20);
  b.writeUInt32LE(size, 24);
  b.writeUInt16LE(name.length, 28);
  b.writeUInt32LE(offset, 42);
  name.copy(b, 46);
  return b;
};

const endOfDirectory = (count: number, size: number, offset: number): Uint8Array => {
  const b = Buffer.alloc(22);
  b.writeUInt32LE(SIG_EOCD, 0);
  b.writeUInt16LE(count, 8);
  b.writeUInt16LE(count, 10);
  b.writeUInt32LE(size, 12);
  b.writeUInt32LE(offset, 16);
  return b;
};

/**
 * Stream a stored (uncompressed) zip of `entries` in one pass: nothing is
 * buffered beyond the central directory, so a bundle can be far larger than
 * memory. Blobs are already arbitrary bytes, so compression would buy little.
 */
// ponytail: no zip64 — a bundle over 4 GB or 65k files needs it.
export const zipStream = <E>(
  entries: ReadonlyArray<ZipEntry<E>>,
  now = new Date(),
): Stream.Stream<Uint8Array, E> =>
  Stream.suspend(() => {
    const { time, date } = dosDateTime(now);
    const central: Buffer[] = [];
    let offset = 0;
    const counted = (s: Stream.Stream<Uint8Array, E>) =>
      Stream.map(s, (chunk) => {
        offset += chunk.byteLength;
        return chunk;
      });

    const files = Stream.fromIterable(entries).pipe(
      Stream.flatMap((entry) =>
        Stream.suspend(() => {
          const name = Buffer.from(entry.name, "utf8");
          const start = offset;
          let crc = 0;
          let size = 0;
          const body = Stream.map(entry.content, (chunk) => {
            crc = crc32(chunk, crc);
            size += chunk.byteLength;
            return chunk;
          });
          const trailer = Stream.suspend(() => {
            central.push(centralHeader(name, time, date, crc, size, start));
            return Stream.make(descriptor(crc, size));
          });
          return counted(
            Stream.make(localHeader(name, time, date)).pipe(
              Stream.concat(body),
              Stream.concat(trailer),
            ),
          );
        }),
      ),
    );

    const directory = Stream.suspend(() => {
      const cd = Buffer.concat(central);
      return Stream.make<Uint8Array[]>(cd, endOfDirectory(central.length, cd.length, offset));
    });

    return Stream.concat(files, directory);
  });
