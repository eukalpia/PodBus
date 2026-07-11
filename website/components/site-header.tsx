'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { useEffect, useState } from 'react';

import { BrandLogo } from '@/components/brand-logo';
import { CloseIcon, GithubIcon, MenuIcon } from '@/components/icons';
import { SearchDialog, type SearchEntry } from '@/components/search-dialog';
import { ThemeToggle } from '@/components/theme-toggle';
import { siteConfig } from '@/lib/site';

export function SiteHeader({ searchEntries }: { searchEntries: SearchEntry[] }) {
  const pathname = usePathname();
  const [menuOpen, setMenuOpen] = useState(false);
  const inDocs = pathname.startsWith('/docs');

  useEffect(() => {
    setMenuOpen(false);
  }, [pathname]);

  useEffect(() => {
    document.body.classList.toggle('mobile-menu-open', menuOpen);
    return () => document.body.classList.remove('mobile-menu-open');
  }, [menuOpen]);

  return (
    <header className="site-header">
      <div className="header-shell">
        <Link className="brand" href="/" aria-label="PodBus home">
          <BrandLogo className="header-brand-logo" />
          {inDocs ? <span className="brand-context">Docs</span> : null}
        </Link>

        <nav className="primary-nav" aria-label="Primary navigation">
          <Link className={pathname === '/' ? 'active' : ''} href="/">
            Overview
          </Link>
          <Link className={inDocs ? 'active' : ''} href="/docs">
            Documentation
          </Link>
          <a href={`${siteConfig.repository}/tree/main/examples`}>Examples</a>
          <a href={`${siteConfig.repository}/releases`}>Releases</a>
        </nav>

        <div className="header-tools">
          <div className="header-search">
            <SearchDialog entries={searchEntries} />
          </div>
          <ThemeToggle />
          <a
            className="icon-button"
            href={siteConfig.repository}
            aria-label="Open PodBus on GitHub"
          >
            <GithubIcon />
          </a>
          <button
            className="icon-button mobile-menu-button"
            type="button"
            aria-label={menuOpen ? 'Close navigation' : 'Open navigation'}
            aria-expanded={menuOpen}
            onClick={() => setMenuOpen((value) => !value)}
          >
            {menuOpen ? <CloseIcon /> : <MenuIcon />}
          </button>
        </div>
      </div>

      {menuOpen ? (
        <nav className="mobile-menu" aria-label="Mobile navigation">
          <Link href="/">Overview</Link>
          <Link href="/docs">Documentation</Link>
          <a href={`${siteConfig.repository}/tree/main/examples`}>Examples</a>
          <a href={`${siteConfig.repository}/releases`}>Releases</a>
          <a href={siteConfig.repository}>GitHub repository</a>
          <div className="mobile-menu-search">
            <SearchDialog entries={searchEntries} />
          </div>
        </nav>
      ) : null}
    </header>
  );
}
