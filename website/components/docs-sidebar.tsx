'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';

import type { DocCategory } from '@/lib/docs-types';

export function DocsSidebar({ categories }: { categories: DocCategory[] }) {
  const pathname = usePathname();

  return (
    <aside className="docs-sidebar" aria-label="Documentation navigation">
      <div className="sidebar-scroll">
        <Link className={`sidebar-overview ${pathname === '/docs/' || pathname === '/docs' ? 'active' : ''}`} href="/docs">
          Documentation overview
        </Link>

        {categories.map((category) => (
          <section key={category.title} className="sidebar-group">
            <h2>{category.title}</h2>
            <nav aria-label={category.title}>
              {category.pages.map((page) => {
                const href = `/docs/${page.slug}`;
                const active = pathname === href || pathname === `${href}/`;
                return (
                  <Link key={page.slug} className={active ? 'active' : ''} href={href}>
                    <span>{page.title}</span>
                    {page.badge ? <em>{page.badge}</em> : null}
                  </Link>
                );
              })}
            </nav>
          </section>
        ))}
      </div>
    </aside>
  );
}
