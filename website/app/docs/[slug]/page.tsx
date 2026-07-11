import type { Metadata } from 'next';
import { notFound } from 'next/navigation';

import { DocRenderer, TableOfContents } from '@/components/doc-renderer';
import { docs, getAdjacentDocs, getDoc } from '@/lib/docs';

export const dynamicParams = false;

export function generateStaticParams() {
  return docs.map((page) => ({ slug: page.slug }));
}

export async function generateMetadata({
  params,
}: {
  params: Promise<{ slug: string }>;
}): Promise<Metadata> {
  const { slug } = await params;
  const page = getDoc(slug);
  if (!page) {
    return {};
  }

  return {
    title: page.title,
    description: page.description,
    alternates: {
      canonical: `/docs/${page.slug}/`,
    },
    openGraph: {
      type: 'article',
      title: `${page.title} — PodBus`,
      description: page.description,
      url: `/docs/${page.slug}/`,
    },
  };
}

export default async function DocumentationPage({
  params,
}: {
  params: Promise<{ slug: string }>;
}) {
  const { slug } = await params;
  const page = getDoc(slug);
  if (!page) {
    notFound();
  }

  const { previous, next } = getAdjacentDocs(slug);

  return (
    <div className="doc-layout">
      <div className="doc-primary">
        <DocRenderer page={page} previous={previous} next={next} />
      </div>
      <TableOfContents page={page} />
    </div>
  );
}
