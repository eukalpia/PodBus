import type { Metadata, Viewport } from 'next';
import type { ReactNode } from 'react';

import { SiteFooter } from '@/components/site-footer';
import { SiteHeader } from '@/components/site-header';
import { searchIndex } from '@/lib/docs';
import { siteConfig } from '@/lib/site';

import './globals.css';
import './visual-refresh.css';
import './brand-polish.css';

const siteUrl = 'https://eukalpia.github.io/PodBus/';
const logoUrl = `${siteUrl}podbus.png`;
const iconUrl = `${siteUrl}podbus-icon.svg`;

export const metadata: Metadata = {
  metadataBase: new URL(siteUrl),
  title: {
    default: 'PodBus — Messaging and durable jobs for Dart',
    template: '%s — PodBus',
  },
  description: siteConfig.description,
  applicationName: 'PodBus Documentation',
  authors: [{ name: 'PodBus contributors' }],
  creator: 'PodBus contributors',
  keywords: [
    'Dart',
    'Serverpod',
    'NATS',
    'JetStream',
    'RabbitMQ',
    'Kafka',
    'message bus',
    'durable jobs',
    'transactional outbox',
  ],
  alternates: {
    canonical: siteUrl,
  },
  openGraph: {
    type: 'website',
    url: siteUrl,
    title: 'PodBus — Messaging and durable jobs for Dart',
    description: siteConfig.description,
    siteName: 'PodBus',
    images: [{ url: logoUrl, alt: 'PodBus' }],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'PodBus — Messaging and durable jobs for Dart',
    description: siteConfig.description,
    images: [logoUrl],
  },
  icons: {
    icon: iconUrl,
    shortcut: iconUrl,
    apple: iconUrl,
  },
};

export const viewport: Viewport = {
  width: 'device-width',
  initialScale: 1,
  colorScheme: 'dark light',
  themeColor: [
    { media: '(prefers-color-scheme: dark)', color: '#05080d' },
    { media: '(prefers-color-scheme: light)', color: '#f4f7fb' },
  ],
};

const themeScript = `
(function () {
  try {
    var saved = localStorage.getItem('podbus-theme');
    var theme = saved === 'light' || saved === 'dark'
      ? saved
      : (window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark');
    document.documentElement.dataset.theme = theme;
  } catch (_) {
    document.documentElement.dataset.theme = 'dark';
  }
})();`;

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en" data-theme="dark" suppressHydrationWarning>
      <head>
        <script dangerouslySetInnerHTML={{ __html: themeScript }} />
      </head>
      <body>
        <a className="skip-link" href="#main-content">Skip to content</a>
        <SiteHeader searchEntries={searchIndex} />
        {children}
        <SiteFooter />
      </body>
    </html>
  );
}
