// Test Script for Dropbox Manifest API
// Usage:
//   node scripts/test-dropbox-manifest.js          (v1.0 with per-file share links)
//   node scripts/test-dropbox-manifest.js --slim   (v1.1 paths_only, no file share links)

const baseDir = '20251113_TestVorname_TestNachname_TA_Pascal_V_Kaethe_L1';

const sharedCustomer = {
  customer_number: 'cust_hash_12345',
  booking_number: 'booking_hash_12345',
  type: 'outside',
  first_name: 'TestVorname',
  last_name: 'TestNachname',
  email: 'test@example.com',
  phone: '+491234567890'
};

const sharedCategories = [
  {
    name: 'Outside_Foto',
    folder_path: `/${baseDir}/Outside_Foto`,
    files: [
      {
        name: 'IMG_001.jpg',
        rel_path: 'Outside_Foto/IMG_001.jpg',
        size: 2048576,
        mime: 'image/jpeg'
      },
      {
        name: 'IMG_002.jpg',
        rel_path: 'Outside_Foto/IMG_002.jpg',
        size: 1987654,
        mime: 'image/jpeg'
      }
    ]
  },
  {
    name: 'Preview_Foto',
    folder_path: `/${baseDir}/Preview_Foto`,
    files: [
      {
        name: 'PREVIEW_001.jpg',
        rel_path: 'Preview_Foto/PREVIEW_001.jpg',
        size: 512000,
        mime: 'image/jpeg'
      }
    ]
  },
  {
    name: 'Outside_Video',
    folder_path: `/${baseDir}/Outside_Video`,
    files: [
      {
        name: 'VID_001.mp4',
        rel_path: 'Outside_Video/VID_001.mp4',
        size: 157286400,
        mime: 'video/mp4'
      }
    ]
  },
  {
    name: 'Preview_Video',
    folder_path: `/${baseDir}/Preview_Video`,
    files: [
      {
        name: 'PREVIEW_001.mp4',
        rel_path: 'Preview_Video/PREVIEW_001.mp4',
        size: 10485760,
        mime: 'video/mp4'
      }
    ]
  }
];

function buildManifestV10() {
  return {
    meta: {
      version: '1.0',
      created_at: new Date().toISOString(),
      uploader_version: '0.0.3.1337'
    },
    customer: sharedCustomer,
    base_dir: baseDir,
    root_folder: {
      path: `/${baseDir}`,
      share_link: 'https://www.dropbox.com/scl/fo/test123/folder?dl=0'
    },
    categories: sharedCategories.map((cat) => ({
      ...cat,
      folder_share_link: `https://www.dropbox.com/scl/fo/test/${cat.name}?dl=0`,
      files: cat.files.map((file, index) => ({
        ...file,
        share_link: `https://www.dropbox.com/scl/fi/test${index}/${file.name}?dl=0`
      }))
    })),
    totals: {
      files_count: 5,
      bytes_total: 172320390
    },
    client_hints: {
      has_previews: true,
      has_videos: true,
      has_photos: true
    }
  };
}

function buildManifestV11Slim() {
  return {
    meta: {
      version: '1.1',
      link_mode: 'paths_only',
      created_at: new Date().toISOString(),
      uploader_version: '0.0.4.1337'
    },
    customer: sharedCustomer,
    base_dir: baseDir,
    root_folder: {
      path: `/${baseDir}`,
      share_link: 'https://www.dropbox.com/scl/fo/test123/folder?dl=0'
    },
    categories: sharedCategories.map((cat) => ({
      ...cat,
      files: cat.files.map((file) => ({
        ...file,
        dropbox_id: `id:slim-test-${file.rel_path.replace(/\//g, '-')}`
      }))
    })),
    totals: {
      files_count: 5,
      bytes_total: 172320390
    },
    client_hints: {
      has_previews: true,
      has_videos: true,
      has_photos: true
    }
  };
}

const useSlim = process.argv.includes('--slim');
const manifest = useSlim ? buildManifestV11Slim() : buildManifestV10();

async function sendManifest(label) {
  const API_KEY = process.env.API_KEY;
  const BASE_URL = process.env.BASE_URL || 'http://localhost:3000';

  if (!API_KEY) {
    console.log('Error: API_KEY environment variable is required!');
    console.log('\nUsage:');
    console.log('  $env:API_KEY="your_api_key"; node scripts/test-dropbox-manifest.js');
    console.log('  $env:API_KEY="your_api_key"; node scripts/test-dropbox-manifest.js --slim');
    process.exit(1);
  }

  console.log(`Sending ${label} manifest to API...`);
  console.log('URL:', `${BASE_URL}/api/orders/create`);
  console.log('Version:', manifest.meta.version);
  console.log('Link mode:', manifest.meta.link_mode || 'per_file');
  console.log('');

  const response = await fetch(`${BASE_URL}/api/orders/create`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${API_KEY}`
    },
    body: JSON.stringify(manifest)
  });

  const data = await response.json();

  // HTTP 202 = Erfolg (async Manifest-Verknuepfung), nicht als Fehler behandeln.
  if (!response.ok && response.status !== 202) {
    console.log('Error!');
    console.log('Status:', response.status);
    console.log('Error:', data);
    return null;
  }

  console.log('Success!');
  console.log('Status:', response.status, data.status || '');
  console.log('Order ID:', data.order_id);
  console.log('Customer URL:', data.final_url);
  return data.order_id;
}

async function testManifestUpload() {
  const label = useSlim ? 'v1.1 slim' : 'v1.0';

  try {
    const orderId = await sendManifest(label);
    if (!orderId) {
      return;
    }

    console.log('\nTesting idempotency (sending same manifest again)...');
    const orderId2 = await sendManifest(`${label} (retry)`);

    if (orderId2 === orderId) {
      console.log('Idempotency works! Same order_id:', orderId2);
    } else {
      console.log('Idempotency failed!');
      console.log('First:', orderId, 'Second:', orderId2);
    }
  } catch (error) {
    console.error('Request failed:', error.message);
  }
}

testManifestUpload();
