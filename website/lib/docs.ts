import { coreConceptDocs } from '@/content/core-concepts';
import { gettingStartedDocs } from '@/content/getting-started';
import { integrationDocs } from '@/content/integrations';
import { operationsDocs } from '@/content/operations';
import { qualificationDocs } from '@/content/qualification';
import { referenceDocs } from '@/content/reference';
import { reliabilityDocs } from '@/content/reliability';
import { transportDocs } from '@/content/transports';
import type { DocCategory, DocPage } from '@/lib/docs-types';

const categoryOrder = [
  'Getting started',
  'Core concepts',
  'Reliability',
  'Transports',
  'Integrations',
  'Operations',
  'Reference',
] as const;

export const docs: DocPage[] = [
  ...gettingStartedDocs,
  ...coreConceptDocs,
  ...reliabilityDocs,
  ...transportDocs,
  ...integrationDocs,
  ...operationsDocs,
  ...qualificationDocs,
  ...referenceDocs,
];

export const docsBySlug = new Map(docs.map((page) => [page.slug, page]));

export const docCategories: DocCategory[] = categoryOrder.map((title, order) => ({
  title,
  order,
  pages: docs
    .filter((page) => page.category === title)
    .sort((left, right) => left.order - right.order),
}));

export function getDoc(slug: string): DocPage | undefined {
  return docsBySlug.get(slug);
}

export function getAdjacentDocs(slug: string): {
  previous?: DocPage;
  next?: DocPage;
} {
  const index = docs.findIndex((page) => page.slug === slug);
  if (index < 0) {
    return {};
  }
  return {
    previous: index > 0 ? docs[index - 1] : undefined,
    next: index < docs.length - 1 ? docs[index + 1] : undefined,
  };
}

export const searchIndex = docs.map((page) => ({
  slug: page.slug,
  title: page.title,
  description: page.description,
  category: page.category,
  headings: page.sections.map((section) => section.title),
  keywords: page.sections.flatMap((section) =>
    section.blocks.flatMap((block) => {
      if (block.type === 'paragraph' || block.type === 'note') {
        return block.text;
      }
      if (block.type === 'bullets') {
        return block.items.join(' ');
      }
      if (block.type === 'steps') {
        return block.items
          .map((item) => `${item.title} ${item.description}`)
          .join(' ');
      }
      if (block.type === 'table') {
        return block.rows.flat().join(' ');
      }
      return '';
    }),
  ),
}));
