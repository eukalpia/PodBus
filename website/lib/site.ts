export const siteConfig = {
  name: 'PodBus',
  version: '0.1.0-beta.1',
  description:
    'Transport-aware messaging and durable jobs for Dart and Serverpod.',
  repository: 'https://github.com/eukalpia/PodBus',
  issues: 'https://github.com/eukalpia/PodBus/issues',
  discussions: 'https://github.com/eukalpia/PodBus/discussions',
  license: 'https://github.com/eukalpia/PodBus/blob/main/LICENSE',
};

export const basePath = process.env.NEXT_PUBLIC_BASE_PATH ?? '';

export function withBasePath(path: string): string {
  if (!path.startsWith('/')) {
    return path;
  }
  return `${basePath}${path}`;
}
