import type { MetadataRoute } from 'next';

import { docs } from '@/lib/docs';

const origin = 'https://eukalpia.github.io/PodBus';

export default function sitemap(): MetadataRoute.Sitemap {
  return [
    {
      url: `${origin}/`,
      changeFrequency: 'weekly',
      priority: 1,
    },
    {
      url: `${origin}/docs/`,
      changeFrequency: 'weekly',
      priority: 0.9,
    },
    ...docs.map((page) => ({
      url: `${origin}/docs/${page.slug}/`,
      changeFrequency: 'weekly' as const,
      priority: page.category === 'Getting started' ? 0.85 : 0.75,
    })),
  ];
}
